#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Classes related to managing the sharing process.

$Id$
"""
from __future__ import print_function, unicode_literals


logger = __import__('logging').getLogger( __name__ )

import collections

from zope import component
from zope.deprecation import deprecate
from zope.cachedescriptors.property import Lazy

from zc import intid as zc_intid

import persistent
import BTrees
from BTrees.OOBTree import OOTreeSet
from ZODB import loglevels

from nti.dataserver.activitystream_change import Change
from nti.dataserver import datastructures
#from nti.dataserver import containers

from nti.externalization.oids import to_external_ntiid_oid

from nti.utils import sets

# TODO: This all needs refactored. The different pieces need to be broken into
# different interfaces and adapters, probably using annotations, to get most
# of this out of the core object structure, and to make more things possible.

def _getObject( intids, intid ):
	return intids.getObject( intid )

class _SCOSContainerFacade(object):
	"""
	Public facade for a single container in
	shared contained object storage. Exists to hide the details
	of looking up an object from its intid number.
	"""

	__parent__ = None
	__name__ = None

	def __init__( self, iiset, allow_missing=False, parent=None, name=None ):
		"""
		:keyword bool allow_missing: If False (the default) then errors will be
			raised for objects that are in the set but cannot be found by id. If
			``True``, then they will be silently ignored.
		"""
		self._container_set = iiset
		self._allow_missing = allow_missing
		if parent:
			self.__parent__ = parent
		if name:
			self.__name__ = name

	def __iter__( self ):
		intids = component.queryUtility( zc_intid.IIntIds )
		for iid in self._container_set:
			__traceback_info__ = iid, self.__parent__, self.__name__
			try:
				yield intids.getObject( iid )
			except KeyError:
				if not self._allow_missing:
					raise
				logger.debug( "Failed to resolve key '%s' in %r of %r", iid, self.__name__, self.__parent__ )

	def __len__( self ):
		return len(self._container_set)

class _SCOSContainersFacade(object):
	"""
	Transient object to implement the `values` support
	by returning each actual value wrapped in a :class:`_SCOSContainerFacade`
	"""

	__parent__ = None
	__name__ = None

	def __init__( self, _containers, allow_missing=False, parent=None, name=None ):
		self._containers = _containers
		self._allow_missing = allow_missing
		if parent:
			self.__parent__ = parent
		if name:
			self.__name__ = name

	def values(self):
		return (_SCOSContainerFacade( v, allow_missing=self._allow_missing, parent=self, name=k )
				for k, v in self._containers.items())

	def __repr__( self ):
		return '<%s %s/%s>' % (self.__class__.__name__, self.__parent__, self.__name__)

_marker = object()
def _getId( contained, when_none=_marker ):
	if contained is None and when_none is not _marker:
		return when_none

	return component.getUtility( zc_intid.IIntIds ).getId( contained )

class _SharedContainedObjectStorage(persistent.Persistent):
	"""
	An object that implements something like the interface of :class:`datastructures.ContainedStorage`,
	but in a simpler form using only intids, and assuming that we never
	need to look objects up by container/localID pairs.
	"""

	family = BTrees.family64

	def __init__( self, family=None ):
		if family is not None:
			self.family = family
		else:
			intids = component.queryUtility( zc_intid.IIntIds )
			if intids:
				self.family = intids.family

		# Map from string container ids to self.family.II.TreeSet
		# The values in the TreeSet are the intids of the shared
		# objects
		self._containers = self.family.OO.BTree()

	def __iter__( self ):
		return iter(self._containers)

	@property
	def containers(self):
		"""
		Returns an object that has a `values` method that iterates
		the dict-like (immutable) containers.
		"""
		return _SCOSContainersFacade( self._containers, allow_missing=True, parent=self, name='SharedContainedObjectStorage' )

	def _check_contained_object_for_storage( self, contained ):
		datastructures.check_contained_object_for_storage( contained )

	def addContainedObject( self, contained ):
		self._check_contained_object_for_storage( contained )

		container_set = self._containers.get( contained.containerId )
		if container_set is None:
			container_set = self.family.II.TreeSet()
			self._containers[contained.containerId] = container_set
		container_set.add( _getId( contained ) )
		return contained

	def deleteEqualContainedObject( self, contained, log_level=None ):
		self._check_contained_object_for_storage( contained )
		container_set = self._containers.get( contained.containerId )
		if container_set is not None:
			if sets.discard_p( container_set, _getId( contained ) ):
				return contained

	def getContainer( self, containerId, defaultValue=None ):
		container_set = self._containers.get( containerId )
		return _SCOSContainerFacade( container_set, allow_missing=True ) if container_set is not None else defaultValue

import struct
def _time_to_64bit_int( value ):
	if value is None: # pragma: no cover
		raise ValueError("For consistency, you must supply the lastModified value" )
	# ! means network byte order, in case we cross architectures
	# anywhere (doesn't matter), but also causes the sizes to be
	# standard, which may matter between 32 and 64 bit machines
	# Q is 64-bit unsigned int, d is 64-bit double
	return struct.unpack( b'!Q', struct.pack( b'!d', value ) )[0]

class _SharedStreamCache(persistent.Persistent):
	"""
	Implements the stream cache for users. Stores activitystream_change.Change
	objects, which are not IContained and don't fit anywhere in the traversal
	tree; hence we avoid the IContained checks.

	We store them keyed by their object's intid. This means that we can only
	store one change per object: the most recent.
	"""
	# TODO: We store Change objects indefinitely while
	# they are in our stream. This keeps a weak ref to
	# the object they are holding, which may otherwise go away.
	# Should we be listening for the intid events and notice when an
	# intid we care about vanishes?
	# TODO: Should the originating user own the change? So that

	family = BTrees.family64
	stream_cache_size = 50

	def __init__( self, family=None ):
		if family is not None: # pragma: no cover
			self.family = family
		else:
			intids = component.queryUtility( zc_intid.IIntIds )
			if intids is not None:
				self.family = intids.family

		# Map from string container ids to self.family.IO.BTree
		# The keys in the BTree leaves are the intids of the objects,
		# the corresponding values are the change
		# TODO: Do this with an IISet and require the Changes to have
		# intids as well. The question then is: Who owns the Change and
		# when does it get deleted?
		self._containers = self.family.OO.BTree()

		# Map from string container ids to self.family.II.BTree
		# The keys are the float times at which we added a change
		# When we fill up a container, we pop the oldest entry using
		# this info, an operation that's efficient on a btree.
		# The float times are converted to their corresponding 64-bit int
		# values-as-bits. I believe these are more-or-less monotonically increasing
		# (TODO: Right?)
		self._containers_modified = self.family.OO.BTree()


	# We use -1 as values for None. This is common in test cases
	# and possibly for deleted objects (there can only be one of these)

	def addContainedObject( self, change ):
		for _containers, factory in ( (self._containers_modified, BTrees.family64.II.BTree),
									  (self._containers, self.family.IO.BTree) ):

			container_map = _containers.get( change.containerId )
			if container_map is None:
				container_map = factory()
				_containers[change.containerId] = container_map

		obj_id = _getId( change.object, -1 )
		old_change = container_map.get( obj_id )
		container_map[obj_id] = change

		# Now save the modification info.
		# Note that container_map is basically a set on change.object, but
		# we might actually get many different changes for a given object.
		# that's why we have to get the one we're replacing (if any)
		# and remove that timestamp from the modified map
		modified_map = self._containers_modified[change.containerId]
		if old_change is not None:
			modified_map.pop( _time_to_64bit_int( old_change.lastModified ), None )

		modified_map[_time_to_64bit_int(change.lastModified)] = obj_id

		# If we're too big, start trimming
		while len(modified_map) > self.stream_cache_size:
			oldest_id = modified_map.pop( modified_map.minKey() )
			container_map.pop( oldest_id ) # If this pop fails, we are somehow corrupted

		return change

	def deleteEqualContainedObject( self, contained, log_level=None ):
		obj_id = _getId( contained )
		modified_map = self._containers_modified.get( contained.containerId )
		if modified_map is not None:
			modified_map.pop( _time_to_64bit_int( contained.lastModified ), None )

		container_map = self._containers.get( contained.containerId )
		if container_map is not None:
			if container_map.pop( obj_id, None ) is not None:
				return contained

	def clearContainer( self, containerId ):
		self._containers.pop( containerId, None )
		self._containers_modified.pop( containerId, None )

	def clear( self ):
		self._containers.clear()
		self._containers_modified.clear()

	def getContainer( self, containerId, defaultValue=None ):
		container_map = self._containers.get( containerId )
		# TODO: If needed, we could get a 'Last Modified' value for
		# this returned object using self._containers_modified
		# NOTE: The returned BTreeItems object does not actually have a
		# __len__ method, as such; even though len() works just fine on it,
		# it means that hamcrest has_length does not work
		return container_map.values() if container_map else defaultValue

	def values( self ):
		for k in self._containers: # Iter the keys and call getContainer to get wrapping1
			yield self.getContainer( k )


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


		# This maintains the strings of external NTIID OIDs whose conversations are muted.
		self.muted_oids = OOTreeSet()



	@Lazy
	def streamCache(self):
		"""
		A cache of recent items that make of the stream. Going back
		further than this requires walking through the containersOfShared.
		"""
		cache = _SharedStreamCache()
		cache.stream_cache_size = self.MAX_STREAM_SIZE
		return cache

	@Lazy
	def containersOfShared(self):
		"""For things that are shared explicitly with me, we maintain a structure
		 that parallels the contained items map. The first level is
		 from container ID to a list of weak references to shared objects.
		 (Un-sharing something, which requires removal from an arbitrary
		 position in the list, should be rare.) Notice that we must NOT
		 have the shared storage set or use IDs, because these objects
		 are not owned by us.
		"""
		# TODO: Might need to set self._p_changed when we do this (cf zope.container.btree)
		# Might also need to add this object to self._p_jar?
		return _SharedContainedObjectStorage()

	@Lazy
	def containers_of_muted(self):
		""" For muted conversations, which can be unmuted, there is an
		identical structure. References are moved in and out of this
		container as conversations are un/muted. The goal of this structure
		is to keep reads fast. Only writes--changing the muted status--are slow"""
		return _SharedContainedObjectStorage()


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
			for obj in container:
				if mute:
					if self.is_muted( obj ):
						to_move.append( obj )
				elif not self.is_muted( obj ):
					to_move.append( obj )

		for x in to_move:
			_from.deleteEqualContainedObject( x )
			_to.addContainedObject( x )

			if mute:
				self.streamCache.deleteEqualContainedObject( x )


	def mute_conversation( self, root_ntiid_oid ):
		self.muted_oids.add( root_ntiid_oid )

		# Now move over anything that is muted
		self.__manage_mute( )


	def unmute_conversation( self, root_ntiid_oid ):
		if sets.discard_p( self.muted_oids, root_ntiid_oid ):
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
		if not source:
			return False
		sets.discard( self._sources_not_accepted,  source.username )
		self._sources_accepted.add( source.username )
		return True

	def stop_accepting_shared_data_from( self, source ):
		if not source:
			return False
		sets.discard( self._sources_accepted, source.username )
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
		if not source:
			return False
		sets.discard( self._sources_accepted, source.username )
		self._sources_not_accepted.add( source.username )
		return True

	def stop_ignoring_shared_data_from( self, source ):
		if not source:
			return False
		sets.discard( self._sources_not_accepted, source.username )
		return True

	def reset_shared_data_from( self, source ):
		"""
		Stop accepting shared data from the `source`, but also do not ignore it.

		This method is usually called on the object on behalf of this
		object.

		:returns: A truth value of whether or not we accepted the
			reset. This implementation returns True if source is valid.
		"""
		if not source:
			return False
		sets.discard( self._sources_accepted, source.username )
		sets.discard( self._sources_not_accepted, source.username )

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

	def reset_accepted_shared_data( self ):
		"""
		Causes this object to forget all accepted users.
		"""
		self._sources_accepted.clear()

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
	# for things I haven't yet accepted but are still shared with me.
	def getSharedContainer( self, containerId, defaultValue=() ):
		"""
		:return: If the containerId is found, an iterable of callable objects (weak refs);
			calling the objects will either return the actual shared object, or None.
		"""
		result = self.containersOfShared.getContainer( containerId, defaultValue )
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
			# Drop the logging to TRACE because at least one of these will be missing
			result = containers.deleteEqualContainedObject( contained, log_level=loglevels.TRACE ) or result
		return result

	def _addToStream( self, change ):
		"""
		:return: A boolean indicating whether the change was accepted
		or muted.
		"""
		if self.is_muted( change.object ):
			return False

		self.streamCache.addContainedObject( change )
		return True

	def _get_stream_cache_containers( self, containerId ):
		""" Return a sequence of stream cache containers for the id. """
		return (self.streamCache.getContainer( containerId, () ),)

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
		result = [self.streamCache.getContainer( containerId, () )]

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
			result.append( [x for x in comm.streamCache.getContainer( containerId, () )
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
			if following is None:
				continue
			if isinstance( following, DynamicSharingTargetMixin ):
				communities_seen.append( following )
				for x in following.getSharedContainer( containerId ):
					if x is not None and not self.is_ignoring_shared_data_from( x.creator ):
						result.append( x )
						result.updateLastModIfGreater( x.lastModified )
			else:
				persons_following.append( following )

		for comm in self._communities:
			comm = self.get_entity( comm )
			if comm is None or comm in communities_seen:
				continue
			for x in comm.getSharedContainer( containerId ):
				if x and x.creator in persons_following:
					result.append( x )
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

def _ii_family():
	intids = component.queryUtility( zc_intid.IIntIds )
	if intids:
		 return intids.family
	return BTrees.family64



class ShareableMixin(datastructures.CreatedModDateTrackingObject):
	""" Represents something that can be shared. It has a set of SharingTargets
	with which it is shared (permissions) and some flags. Only its creator
	can alter its sharing targets. It may be possible to copy this object. """

	# An IITreeSet of string userids
	# TODO: FIXME: When the user is deleted and his ID goes bad, we're
	# not listening for that. What if the ID gets reused for something else?
	_sharingTargets = None

	def __init__( self ):
		super(ShareableMixin,self).__init__()



	def clearSharingTargets( self ):
		if self._sharingTargets is not None:
			self._sharingTargets.clear() # Preserve existing object

			self.updateLastMod()

	def addSharingTarget( self, target, actor=None ):
		"""
		Adds a sharing target. We accept either SharingTarget
		subclasses, or iterables of them.

		"""
		if isinstance( target, basestring ):
			raise TypeError('Strings are no longer acceptable', target, self)

		if isinstance( target, collections.Iterable ) \
			   and not isinstance( target, basestring ) \
			   and not isinstance( target, DynamicSharingTargetMixin ):
			# TODO: interfaces
			# expand iterables now
			for t in target:
				self.addSharingTarget( t, actor=actor )
			return

		# Don't allow sharing with ourself, it's weird
		# Allow self.creator to be  string or an Entity
		if self.creator == target:
			logger.debug( "Dissalow sharing object with creator %s", self.creator )
			return

		if self._sharingTargets is None:
			self._sharingTargets = _ii_family().II.TreeSet()

		self._sharingTargets.add( _getId( target ) )
		self.updateLastMod()

	def updateSharingTargets( self, replacement_targets ):
		"""
		Cause this object to be shared with only the `replacement_targets` and
		no one else.

		:param replacement_targets: A collection of users.
		"""

		replacement_userids = _ii_family().II.TreeSet()
		def addToSet( target ):
			if isinstance( target, basestring ):
				raise TypeError('Strings are no longer acceptable', target, self)

			if target == self.creator:
				return

			# TODO: interfaces
			if isinstance(target, DynamicSharingTargetMixin):
				replacement_userids.add( _getId( target ) )
			elif isinstance( target, collections.Iterable ):
				for x in target:
					addToSet( x )
			else:
				replacement_userids.add( _getId( target ) )

		for target in replacement_targets:
			if target is None:
				continue
			addToSet( target )

		if not replacement_userids:
			self.clearSharingTargets()
			return

		if self._sharingTargets is None:
			self._sharingTargets = replacement_userids
		else:
			self._sharingTargets.update( replacement_userids )

		# Now remove any excess

		# If for some reason we don't actually have sharing targets
		# then this may return None
		excess_targets = _ii_family().II.difference( self._sharingTargets, replacement_userids )
		for x in (excess_targets or ()):
			self._sharingTargets.remove( x )


	def isSharedWith( self, wants ):
		""" Checks if we are shared with `wants`, which must be a
		Principal."""
		if not self._sharingTargets:
			return False

		try:
			return _getId( wants ) in self._sharingTargets
		except KeyError:
			pass


	def getFlattenedSharingTargetNames(self):
		"""
		Returns a flattened :class:`set` of :class:`SharingTarget` usernames with whom this item
		is shared.
		"""
		if self._sharingTargets is None:
			return set()
		return set( (x.username for x in _SCOSContainerFacade( self._sharingTargets, allow_missing=True ) ) )

	flattenedSharingTargetNames = property( getFlattenedSharingTargetNames )
	getFlattenedSharingTargetNames = deprecate("Prefer 'flattenedSharingTargetNames' attribute")(getFlattenedSharingTargetNames)


	@property
	def sharingTargets(self):
		"""
		Returns a flattened :class:`set` of entities with whom this item
		is shared.
		"""
		if self._sharingTargets is None:
			return set()
		# Provide a bit of defense against the intids going away or changing
		# out from under us
		return set( (x for x in _SCOSContainerFacade( self._sharingTargets, allow_missing=True )
					if x is not None and hasattr( x, 'username') ) )
