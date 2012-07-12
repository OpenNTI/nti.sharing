#!/usr/bin/env python2.7

import logging
logger = logging.getLogger( __name__ )


import numbers
import collections

from zope import interface
from zope import component


from BTrees.OOBTree import OOTreeSet, OOBTree
from ZODB import loglevels

from nti.dataserver.activitystream_change import Change
from nti.dataserver import datastructures
from nti.dataserver import containers

from nti.externalization.persistence import PersistentExternalizableList, PersistentExternalizableWeakList
from nti.externalization.oids import to_external_ntiid_oid


class SharingTargetMixin(object):
	"""
	Something that is a holder of shared data. These objects
	may be "passive." Implementations must provide the `username` field
	and total ordering. (TODO: interface).

	**Sharing Model**

	In general, the sharing relationship has to be consumated at both sides.
	Relationships are between the entity doing the sharing (the *source*),
	and the entity getting the shared data (the *target*) organizational
	structures within the source (e.g., *friends lists* are irrelevant. This
	relationship is called the *accepts* relationship.

	It is assumed that targets will accept shared data by default, but they
	may choose to opt out of any relationship. The opt out status of a
	target is retained so that future relationship requests from a
	previously opted-out entity are **not** accepted by default.

	Another relationship is the *follows* relationship. A source may share
	data with another source or with an entire community (such as
	*Everyone*). The follows relationship is relevant only for the latter
	situation. Follows relationships are between an entity and a community
	or specific source. When an entity follows a community, it receives
	everything shared with that community. When an entity follows an
	individual, it receives things shared by that individual *to communities
	the entity is a member of.* Note that follows is a one-way
	relationship. The source implicitly granted permission when it shared
	something with the community.

	"""

	MAX_STREAM_SIZE = 50

	def __init__( self, *args, **kwargs ):
		super(SharingTargetMixin,self).__init__( *args, **kwargs )
		# Notice that I'm still working out
		# how best to reference other persistent objects, by
		# username/id or by weakrefs to the object itself. Thus
		# the inconsistencies.

		self._sources_not_accepted = OOTreeSet()
		#Set us usernames we won't accept shared data from. Also applies to
		#things pulled from communities.

		self._sources_accepted = OOTreeSet()
		#Set of usernames that we'll accept explicitly shared data
		#from. Notice that acceptance/not acceptance is completely on
		#our side of things; the sender never knows--our 'ignore' is a
		#quiet ignore.


		# For things that are shared explicitly with me, we maintain a structure
		# that parallels the contained items map. The first level is
		# from container ID to a list of weak references to shared objects.
		# (Un-sharing something, which requires removal from an arbitrary
		# position in the list, should be rare.) Notice that we must NOT
		# have the shared storage set or use IDs, because these objects
		# are not owned by us.
		# TODO: Specialize these data structures
		self.containersOfShared = datastructures.ContainedStorage( weak=True,
																   create=False,
																   containerType=containers.EventlessLastModifiedBTreeContainer,
																   set_ids=False )

		# For muted conversations, which can be unmuted, there is an
		# identical structure. References are moved in and out of this
		# container as conversations are un/muted. The goal of this structure
		# is to keep reads fast. Only writes--changing the muted status--are slow
		self.containers_of_muted = datastructures.ContainedStorage( weak=True,
																   create=False,
																   containerType=containers.EventlessLastModifiedBTreeContainer,
																   set_ids=False )
		# This maintains the strings of external NTIID OIDs whose conversations are muted.
		self.muted_oids = OOTreeSet()

		# These items are not using 'real' containers, so they don't become parents,
		# so giving them navigable names is safe (and pretty)
		self.containersOfShared.__name__ = '++containersOfShared'
		self.containers_of_muted.__name__ = '++containersOfMuted'

		# A cache of recent items that make of the stream. Going back
		# further than this requires walking through the containersOfShared.
		# Map from containerId -> PersistentExternalizableWeakList
		# TODO: Rethink this. It's terribly inefficient.
		self.streamCache = OOBTree()

	def _discard( self, s, k ):
		try:
			s.remove( k )
			self._p_changed = True
			return True
		except KeyError:
			return False

	def __manage_mute( self, mute=True ):
		# TODO: Horribly inefficient
		if self._p_jar and self.containersOfShared._p_jar:
			self._p_jar.readCurrent( self.containersOfShared )
			self._p_jar.readCurrent( self.containers_of_muted )
		_from = self.containersOfShared
		_to = self.containers_of_muted
		if not mute:
			_from, _to = _to, _from

		to_move = []
		for container in _from.containers.values():
			if isinstance( container, numbers.Number ): continue
			for obj in container:
				# TODO: Temporary migration code. Old objects had lists as `container`
				# while the new objects are BTrees. Do a database migration when the changes
				# are finished.
				if isinstance( obj, basestring ): obj = container[obj]()

				if mute:
					if self.is_muted( obj ):
						to_move.append( obj )
				elif not self.is_muted( obj ):
					to_move.append( obj )


		for x in to_move:
			_from.deleteEqualContainedObject( x )
			_to.addContainedObject( x )

			if not mute: continue

			stream = self.streamCache.get( x.containerId )
			if stream is not None:
				change = None
				for change in stream:
					if change.object == x:
						break
				if change is not None:
					stream.remove( change )

	def mute_conversation( self, root_ntiid_oid ):
		self.muted_oids.add( root_ntiid_oid )

		# Now move over anything that is muted
		self.__manage_mute( )


	def unmute_conversation( self, root_ntiid_oid ):
		if self._discard( self.muted_oids, root_ntiid_oid ):
			# Now unmute anything required
			self.__manage_mute( mute=False )


	def is_muted( self, the_object ):
		if the_object is None:
			return False

		if getattr( the_object, 'id', self ) in self.muted_oids:
			return True
		ntiid = to_external_ntiid_oid( the_object )
		if ntiid in self.muted_oids:
			return True
		reply_ntiid = to_external_ntiid_oid( the_object.inReplyTo ) if hasattr( the_object, 'inReplyTo' ) else None
		if reply_ntiid in self.muted_oids:
			return True
		refs_ntiids = [to_external_ntiid_oid(x) for x in the_object.references] if hasattr( the_object, 'references') else ()
		for x in refs_ntiids:
			if x in self.muted_oids:
				return True

		return False

	def accept_shared_data_from( self, source ):
		"""
		Begin accepting shared data from the `source`.

		If the `source` is being ignored, it will no longer be ignored.
		This method is usually called on this object by (on behalf of) `source`
		itself.
		This relationship persists until terminated, it doesn't cease simply
		because the `source` deleted the original friends list (circle).

		:returns: A truth value of whether or not we actually are now
			accepting shared data. This class always returns True if
			`source` is valid, subclasses may differ (this class doesn't
			implement ignoring).
		"""
		if not source: return False
		self._discard( self._sources_not_accepted,  source.username )
		self._sources_accepted.add( source.username )
		# FIXME: Why are we having to do this?
		self._p_changed = True
		return True

	def stop_accepting_shared_data_from( self, source ):
		if not source: return False
		self._discard( self._sources_accepted, source.username )
		return True

	@property
	def accepting_shared_data_from( self ):
		""" :returns: Iterable names of entities we accept shared data from. """
		return set(self._sources_accepted)

	def ignore_shared_data_from( self, source ):
		"""
		The opposite of :meth:`accept_shared_data_from`.

		This method is usually called on the object on behalf of this
		object (e.g., by the user this object represents).
		"""
		if not source: return False
		self._discard( self._sources_accepted, source.username )
		self._sources_not_accepted.add( source.username )
		self._p_changed = True
		return True

	def stop_ignoring_shared_data_from( self, source ):
		if not source: return False
		self._discard( self._sources_not_accepted, source.username )
		return True

	def reset_shared_data_from( self, source ):
		"""
		Stop accepting shared data from the `source`, but also do not ignore it.

		This method is usually called on the object on behalf of this
		object.

		:returns: A truth value of whether or not we accepted the
			reset. This implementation returns True if source is valid.
		"""
		if not source: return False
		self._discard( self._sources_accepted, source.username )
		self._discard( self._sources_not_accepted, source.username )

	def reset_all_shared_data( self ):
		"""
		Causes this object to forget all sharing and ignoring settings.
		"""
		# Keep the same objects in case of references
		self.reset_ignored_shared_data()
		self.reset_accepted_shared_data()

	def reset_ignored_shared_data( self ):
		"""
		Causes this object to forget all ignored settings.
		"""
		self._sources_not_accepted.clear()
		self._p_changed = True

	def reset_accepted_shared_data( self ):
		"""
		Causes this object to forget all accepted users.
		"""
		self._sources_accepted.clear()
		self._p_changed = True

	@property
	def ignoring_shared_data_from( self ):
		""" :returns: Iterable of names of entities we are specifically ignoring shared data from. """
		return set(self._sources_not_accepted)

	def is_accepting_shared_data_from( self, source ):
		"""
		Return if this object is accepting data that is explicitly
		shared with it by `source`.
		"""
		return (source.username if hasattr(source, 'username') else source) in self._sources_accepted

	def is_ignoring_shared_data_from( self, source ):
		"""
		The opposite of :meth:`is_accepting_shared_data_from`
		"""
		# Naturally we ignore ourself
		username = source.username if hasattr(source, 'username') else source
		return username == self.username or username in self._sources_not_accepted

	# TODO: In addition to the actual explicitly shared objects that I've
	# accepted because I'm not ignoring, we need the "incoming" group
	# for things I haven't yet accepted by are still shared with me.
	def getSharedContainer( self, containerId, defaultValue=() ):
		"""
		:return: If the containerId is found, an iterable of callable objects (weak refs);
			calling the objects will either return the actual shared object, or None.
		"""
		result = self.containersOfShared.getContainer( containerId, defaultValue )
		# TODO: Temporary migration code
		if isinstance( result, containers.EventlessLastModifiedBTreeContainer ):
			result = result.values()
		return result

	def _addSharedObject( self, contained ):
		containers = self.containers_of_muted if self.is_muted(contained) else self.containersOfShared
		containers.addContainedObject( contained )

	def _removeSharedObject( self, contained ):
		"""
		:return: The removed object, or None if nothing was removed.
		"""
		# Remove from both muted and normal, just in case
		result = False
		for containers in (self.containersOfShared,self.containers_of_muted):
			# Drop the logging to trace because at least one of these will be missing
			result = containers.deleteEqualContainedObject( contained, log_level=loglevels.TRACE ) or result
		return result

	def _addToStream( self, change ):
		"""
		:return: A boolean indicating whether the change was accepted
		or muted.
		"""
		if self.is_muted( change.object ):
			return False

		container = self.streamCache.get( change.containerId )
		if container is None:
			container = PersistentExternalizableWeakList()
			self.streamCache[change.containerId] = container
		if len(container) >= self.MAX_STREAM_SIZE:
			container.pop( 0 )

		container.append( change )
		return True

	def _get_stream_cache_containers( self, containerId ):
		""" Return a sequence of stream cache containers for the id. """
		return (self.streamCache.get( containerId, () ),)

	def getContainedStream( self, containerId, minAge=-1, maxCount=MAX_STREAM_SIZE ):
		# The contained stream is an amalgamation of the traffic explicitly
		# to us, plus the traffic of things we're following. We merge these together and return
		# just the ones that fit the criteria.
		# TODO: What's the right heuristic here? Seems like things shared directly with me
		# may be more important than things I'm following...
		# TODO: These data structures could and should be optimized for this.
		result = datastructures.LastModifiedCopyingUserList()

		containers = self._get_stream_cache_containers( containerId )

		def add( item, lm=None ):
			lm = lm or item.lastModified
			result.append( item )
			result.updateLastModIfGreater( lm )

		for container in containers:
			for item in container:
				if (item and item.lastModified > minAge
					and not self.is_ignoring_shared_data_from( item.creator ) ):
					add( item )

					if len( result ) > maxCount:
						return result

		# If we get here, then we weren't able to satisfy the request from the caches. Must walk
		# through the shared items directly.
		# We should probably be able to satisfy the request from the people we
		# follow. If not, we try to fill in with everything shared with us/followed by us
		# being careful to avoid duplicating things present in the stream
		# TODO: We've lost change information for these items.
		def dup( item ):
			for x in result:
				if x.object == item: return True
			return False
		for item in self.getSharedContainer( containerId ):
			# These items are callables, weak refs
			item = item()
			if item and item.lastModified > minAge and not dup( item ):
				change = Change( Change.SHARED, item )
				change.creator = item.creator or self

				# Since we're fabricating a change for this item,
				# we know it can be no later than when the item itself was last changed
				change.lastModified = item.lastModified

				add( change, item.lastModified )

				if len(result) > maxCount:
					break
		# We'll we've done the best that we can.
		return result

	def _acceptIncomingChange( self, change ):
		"""
		:return: A value indicating if the change was actually accepted or
		is muted.
		"""
		accepted = self._addToStream( change )
		# TODO: What's the right check here?
		if not hasattr( change.object, 'username' ):
			self._addSharedObject( change.object )
		return accepted

	def _noticeChange( self, change ):
		""" Should run in a transaction. """
		# We hope to only get changes for objects shared with us, but
		# we double check to be sure--DELETES must always go through.

		if change.type in (Change.CREATED,Change.SHARED):
			if (change.object is not None
				and change.object.isSharedWith( self )
				and self.is_accepting_shared_data_from( change.creator )) :
				self._acceptIncomingChange( change )
		elif change.type == Change.MODIFIED:
			if change.object is not None:
				if change.object.isSharedWith( self ):
					# NOTE: Each change is going into the stream
					# leading to the possibility of multiple of the same objects
					# in the stream.
					# We should NOT have duplicates in the shared objects,
					# though, because we're maintaining that as a map keyed by
					# IDs. The container does detect and abort attempts to insert
					# duplicate keys before the original is removed, so
					# order matters
					self._addToStream( change )
					self._addSharedObject( change.object )
				else:
					# FIXME: Badly linear
					self._removeSharedObject( change.object )
		elif change.type == Change.DELETED:
			# The weak refs would clear eventually.
			# For speedy deletion at the expense of scale, we
			# can force the matter
			removed = self._removeSharedObject( change.object )
			if removed is False or removed is None: # Explicit, not falsey
				logger.warn( "Incoming deletion for object not found %s", change )
		elif change.type == Change.CIRCLED:
			self._acceptIncomingChange( change )
		# Do a dual-dispatch to notify complex subscribers that need to know
		# the destination user
		component.subscribers( (self, change), None )


class SharingSourceMixin(SharingTargetMixin):
	"""
	Something that can share data. These objects are typically
	"active."
	"""

	def __init__( self, *args, **kwargs ):
		super(SharingSourceMixin,self).__init__( *args, **kwargs )
		# Notice that I'm still working out
		# how best to reference other persistent objects, by
		# username/id or by weakrefs to the object itself. Thus
		# the inconsistencies.

		self._communities = OOTreeSet()
		#Set of usernames of communities we belong to.


		self._following = OOTreeSet()
		# Set of entity names we want to follow.
		# For users, we will source data specifically
		# from them out of communities we belong to. For communities, we will
		# take all data (with the exception of _sources_not_accepted, of course.

	def follow( self, source ):
		""" Adds `source` to the list of followers. """
		self._following.add( source.username )
		return True

	@property
	def following(self):
		""" :returns: Iterable names of entities we are following. """
		return set(self._following)

	def join_community( self, community ):
		""" Marks this object as a member of `community.` Does not follow `community`.
		:returns: Whether we are now following the community. """
		self._communities.add( community.username )
		return True

	@property
	def communities( self ):
		""" :returns: Iterable names of communities we belong to. """
		return set(self._communities)

	def _get_stream_cache_containers( self, containerId ):
		# start with ours
		result = [self.streamCache.get( containerId, () )]

		# add everything we follow. If it's a community, we take the
		# whole thing (ignores are filtered in the parent method). If
		# it's a person, we take stuff they've shared to communities
		# we're a member of

		persons_following = []
		for following in self._following:
			following = self.get_entity( following )
			if following is None: continue
			if isinstance( following, DynamicSharingTargetMixin ):
				result += following._get_stream_cache_containers( containerId )
			else:
				persons_following.append( following )

		for comm in self._communities:
			comm = self.get_entity( comm )
			if comm is None: continue
			result.append( [x for x in comm.streamCache.get( containerId, () )
							if x is not None and x.creator in persons_following] )


		return result

	def getSharedContainer( self, containerId, defaultValue=() ):
		# start with ours
		result = datastructures.LastModifiedCopyingUserList()
		super_result = super(SharingSourceMixin,self).getSharedContainer( containerId, defaultValue=defaultValue )
		if super_result is not None and super_result is not defaultValue:
			result.extend( super_result )

		# add everything we follow. If it's a community, we take the whole
		# thing (minus ignores). If it's a person, we take stuff they've shared to
		# communities we're a member of (ignores not an issue).
		# Note that to be consistent with the super class interface, we do not
		# de-ref the weak refs in the returned value (even though we must de-ref them
		# internally)
		# TODO: This needs much optimization. And things like paging will
		# be important.

		persons_following = []
		communities_seen = []
		for following in self._following:
			following = self.get_entity( following )
			if following is None: continue
			if isinstance( following, DynamicSharingTargetMixin ):
				communities_seen.append( following )
				for ref in following.getSharedContainer( containerId ):
					x = ref()
					if x is not None and not self.is_ignoring_shared_data_from( x.creator ):
						result.append( ref )
						result.updateLastModIfGreater( x.lastModified )
			else:
				persons_following.append( following )

		for comm in self._communities:
			comm = self.get_entity( comm )
			if comm is None or comm in communities_seen: continue
			for ref in comm.getSharedContainer( containerId ):
				x = ref()
				if x and x.creator in persons_following:
					result.append( ref )
					result.updateLastModIfGreater( x.lastModified )

		# If we made no modifications, return the default
		# (which would have already been returned by super; possibly it returned other data)
		if not result:
			return super_result
		return result


class DynamicSharingTargetMixin(SharingTargetMixin):
	"""
	Instances represent communities or collections (e.g., tags)
	that a user might want to 'follow' or subscribe to.

	Since they don't represent individuals, they always accept 'subscribe'
	requests. They also don't generate any notifications.
	"""

	defaultGravatarType = 'retro'

	MAX_STREAM_SIZE = 100000
	# Turns out we need to maintain both the stream and the objects.
	def __init__(self, *args, **kwargs):
		super(DynamicSharingTargetMixin,self).__init__( *args, **kwargs )


class ShareableMixin(datastructures.CreatedModDateTrackingObject):
	""" Represents something that can be shared. It has a set of SharingTargets
	with which it is shared (permissions) and some flags. Only its creator
	can alter its sharing targets. It may be possible to copy this object. """

	def __init__( self ):
		super(ShareableMixin,self).__init__()
		# Our set of targets we are shared with. If we
		# have a creator, then only the creator can alter these.
		self._sharingTargets = None

	@property
	def sharingTargets(self):
		return self._sharingTargets if self._sharingTargets is not None else ()

	def clearSharingTargets( self ):
		self._sharingTargets = None
		self.updateLastMod()

	def addSharingTarget( self, target, actor=None ):
		""" Adds a sharing target. We accept either SharingTarget
		subclasses, or strings, or iterables of strings."""
		if isinstance( target, collections.Iterable ) \
			   and not isinstance( target, basestring ) \
			   and not hasattr( target, 'username' ):
			# expand iterables now
			for t in target: self.addSharingTarget( t, actor=actor )
			return

		if self.creator is not None and self.creator != actor:
			raise ValueError( "Creator (%s) is not actor (%s)" % (self.creator,actor) )
		if self._sharingTargets is None:
			self._sharingTargets = PersistentExternalizableList()
		if target not in self._sharingTargets:
			# Don't allow sharing with ourself, it's weird
			# Allow self.creator to be  string or an Entity
			if not self.creator or (self.creator != target
									and getattr(self.creator, 'username', self.creator) != target):
				self._sharingTargets.append( target )
		# if we ourselves are persistent, mark us changed
		if hasattr( self, '_p_changed' ):
			setattr( self, '_p_changed', True )

		self.updateLastMod()

	def isSharedWith( self, wants ):
		""" Checks if we are shared with `wants`, which can be a
		Principal or a string."""
		if not self._sharingTargets:
			return False

		if not isinstance( wants, basestring ):
			# because our list has strings in it,
			# it's easiest to get this as a string now
			wants = wants.username

		return wants in self.getFlattenedSharingTargetNames()

	def getFlattenedSharingTargetNames(self):
		""" Returns a flattened :class:`set` of :class:`SharingTarget` usernames with whom this item
		is shared."""
		sharingTargetNames = set()

		def addToSet( target ):
			if isinstance( target, basestring ):
				sharingTargetNames.add( target )
			elif isinstance( target, collections.Iterable ):
				for x in target: addToSet( x )
			else:
				sharingTargetNames.add( target.username )

		for target in self.sharingTargets:
			addToSet( target )

		return sharingTargetNames
