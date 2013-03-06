#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Classes related to managing the sharing process.

$Id$
"""
from __future__ import print_function, unicode_literals

logger = __import__('logging').getLogger( __name__ )

import six
import heapq
import collections

from zope import component
from zope import interface
from zope.location import locate
from zope.deprecation import deprecate
from zope.container.contained import Contained
from zope.cachedescriptors.property import Lazy

from zc import intid as zc_intid
from nti.intid.containers import IntidResolvingIterable
from nti.intid.containers import IntidContainedStorage

import BTrees
import persistent
from ZODB import loglevels
from BTrees.OOBTree import OOTreeSet
from ZODB.POSException import POSKeyError

from nti.dataserver import datastructures
from nti.dataserver.activitystream_change import Change
from nti.dataserver import interfaces as nti_interfaces

from nti.externalization.oids import to_external_ntiid_oid

from nti.utils import sets



# TODO: This all needs refactored. The different pieces need to be broken into
# different interfaces and adapters, probably using annotations, to get most
# of this out of the core object structure, and to make more things possible.


_marker = object()
def _getId( contained, when_none=_marker ):
	if contained is None and when_none is not _marker:
		return when_none

	return component.getUtility( zc_intid.IIntIds ).getId( contained )

class _SharedContainedObjectStorage(IntidContainedStorage):
	"""
	An object that implements something like the interface of :class:`datastructures.ContainedStorage`,
	but in a simpler form using only intids, and assuming that we never
	need to look objects up by container/localID pairs.
	"""

	# This class exists for backwards compatibilty in pickles, and
	# to override the contained object check

	def _check_contained_object_for_storage( self, contained ):
		datastructures.check_contained_object_for_storage( contained )



import struct
def _time_to_64bit_int( value ):
	if value is None: # pragma: no cover
		raise ValueError("For consistency, you must supply the lastModified value" )
	# ! means network byte order, in case we cross architectures
	# anywhere (doesn't matter), but also causes the sizes to be
	# standard, which may matter between 32 and 64 bit machines
	# Q is 64-bit unsigned int, d is 64-bit double
	return struct.unpack( b'!Q', struct.pack( b'!d', value ) )[0]

class _SharedStreamCache(persistent.Persistent,Contained):
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
	# TODO: Should the originating user own the change? That way it picks
	# a shard, and it goes away when the user does

	family = BTrees.family64
	stream_cache_size = 50

	def __init__( self, family=None ):
		super(_SharedStreamCache,self).__init__()
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

		# TODO: I'm supposed to be storing a Length object separately and maintaing
		# it for each BTree, as asking for their len() can be expensive

	def _read_current( self, container ):
		if self._p_jar and getattr( container, '_p_jar', None ):
			self._p_jar.readCurrent( container )

	# We use -1 as values for None. This is common in test cases
	# and possibly for deleted objects (there can only be one of these)

	def addContainedObject( self, change ):
		for _containers, factory in ( (self._containers_modified, BTrees.family64.II.BTree),
									  (self._containers, self.family.IO.BTree) ):
			self._read_current( _containers )
			container_map = _containers.get( change.containerId )
			self._read_current( container_map )
			if container_map is None:
				container_map = factory()
				_containers[change.containerId] = container_map
		# And so at this point, `container_map` is an IOBTree

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
			# If this pop fails, we are somehow corrupted, in that our state
			# doesn't match. It's a relatively minor corruption, however,
			# (the worst that happens is that the stream cache gets a bit too big) so
			# the most it entails is logging.
			# In the future we may need to rebuild datastructures that exhibit
			# this problem. their stream may need to be cleared
			try:
				container_map.pop( oldest_id )
			except KeyError:
				logger.debug( "Failed to pop oldest object with id %s in %s",
							  oldest_id, self )

		# Change objects can wind up sent to multiple people in different shards
		# They need to have an owning shard, otherwise it's not possible to pick
		# one if they are reachable from multiple. So we add them here
		# TODO: See comments above about making the user own these
		if self._p_jar and getattr( change, '_p_jar', self ) is None:
			self._p_jar.add( change )

		return change

	def deleteEqualContainedObject( self, contained, log_level=None ):
		self._read_current( self._containers_modified )
		obj_id = _getId( contained )
		modified_map = self._containers_modified.get( contained.containerId )
		if modified_map is not None:
			self._read_current( modified_map )
			modified_map.pop( _time_to_64bit_int( contained.lastModified ), None )

		self._read_current( self._containers )
		container_map = self._containers.get( contained.containerId )
		if container_map is not None:
			self._read_current( container_map )
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
		# Iter the keys and call getContainer to get wrapping
		for k in self._containers:
			# FIXME: What? There's no wrapping anymore
			yield self.getContainer( k )

	def keys( self ):
		return self._containers.keys()

	def __iter__( self ):
		return iter(self._containers)

	def __repr__( self ):
		return '<%s at %s/%s>' % (self.__class__.__name__, self.__parent__, self.__name__ )

def _set_of_usernames_from_named_lazy_set_of_wrefs(self, name):
	container = ()
	self._p_activate() # Ensure we have a dict
	if name in self.__dict__:
		container = getattr( self, name )
	result = set()
	for wref in container:
		val = wref()
		if val is not None:
			result.add( val.username )

	return result

def _iterable_of_entities_from_named_lazy_set_of_wrefs(self, name):
	container = ()
	self._p_activate() # Ensure we have a dict
	if name in self.__dict__:
		container = getattr( self, name )
	for wref in container:
		val = wref()
		if val is not None:
			yield val

def _remove_entity_from_named_lazy_set_of_wrefs( self, name, entity ):
	self._p_activate() # Ensure we have a dict
	if name in self.__dict__:
		jar = getattr( self, '_p_jar', None )
		container = getattr( self, name )
		if jar is not None:
			# Since we're mutating, probably based on some other object's
			# content, make sure we're mutating the current version
			jar.readCurrent( self )
			jar.readCurrent( container )
		sets.discard( container, nti_interfaces.IWeakRef( entity ) )

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


	.. todo:: This needs to be reworked to not require inheritance
		and direct storage on the entity objects. It can be pulled apart
		into smaller pieces.

	"""

	MAX_STREAM_SIZE = 50

	def __init__( self, *args, **kwargs ):
		super(SharingTargetMixin,self).__init__( *args, **kwargs )

	# Note that @Lazy mutates the __dict__ directly, bypassing
	# __setattribute__, which means we are responsible for marking
	# ourself changed

	@Lazy
	def streamCache(self):
		"""
		A cache of recent items that make of the stream. Going back
		further than this requires walking through the containersOfShared.
		"""
		cache = _SharedStreamCache()
		cache.stream_cache_size = self.MAX_STREAM_SIZE
		self._p_changed = True
		if self._p_jar:
			self._p_jar.add( cache )
		locate(cache, self, 'streamCache')
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
		self._p_changed = True
		result = _SharedContainedObjectStorage()
		if self._p_jar:
			self._p_jar.add( result )
		locate(result, self, 'containersOfShared')
		return result

	@Lazy
	def containers_of_muted(self):
		""" For muted conversations, which can be unmuted, there is an
		identical structure. References are moved in and out of this
		container as conversations are un/muted. The goal of this structure
		is to keep reads fast. Only writes--changing the muted status--are slow"""
		self._p_changed = True
		result = _SharedContainedObjectStorage()
		if self._p_jar:
			self._p_jar.add( result )
		locate(result, self, 'containers_of_muted')
		return result

	def _lazy_create_ootreeset_for_wref(self):
		self._p_changed = True
		result = OOTreeSet()
		if self._p_jar:
			self._p_jar.add( result )
		return result

	@Lazy
	def _muted_oids(self):
		"""
		Maintains the strings of external NTIID OIDS whose conversations are
		muted. (Can actually hold any type of NTIID.)
		"""
		return self._lazy_create_ootreeset_for_wref()

	@Lazy
	def _entities_not_accepted(self):
		"""
		Set of weak refs to entities that we won't accept shared data from.
		Also applies to things pulled from communities.
		"""
		return self._lazy_create_ootreeset_for_wref()

	@Lazy
	def _entities_accepted(self):
		"""
		Set of weak refs to entities that we'll accept explicitly shared data from.
		Notice that acceptance/not acceptance is completely on our side of things;
		the sender never knows---our 'ignore' is a quiet ignore.
		"""
		return self._lazy_create_ootreeset_for_wref()

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

		# There is the possibility for things to be in the stream
		# cache that were never in the shared objects (forums)
		# However, we have to apply muting at read time anyway, so
		# there's no point going through the containers now

	def mute_conversation( self, root_ntiid_oid ):
		"""
		:raises TypeError: If `root_ntiid_oid` is ``None``.
		"""
		# self._muted_oids would raise TypeError, but no need to
		# run the lazy creator if not needed
		if root_ntiid_oid is None: raise TypeError('Object has default comparison') # match BTree message
		self._muted_oids.add( root_ntiid_oid )

		# Now move over anything that is muted
		self.__manage_mute( )


	def unmute_conversation( self, root_ntiid_oid ):
		if '_muted_oids' not in self.__dict__ or root_ntiid_oid is None:
			# No need to let self._muted_oids raise TypeError if root_ntiid_oid is
			# None: It could never possibly be muted
			return

		if sets.discard_p( self._muted_oids, root_ntiid_oid ):
			# Now unmute anything required
			self.__manage_mute( mute=False )


	def is_muted( self, the_object ):
		if the_object is None or '_muted_oids' not in self.__dict__:
			return False

		if getattr( the_object, 'id', self ) in self._muted_oids:
			return True
		ntiid = to_external_ntiid_oid( the_object )
		#__traceback_info__ = the_object, ntiid
		if ntiid in self._muted_oids: # Raises TypeError if ntiid is None; which shouldn't happen
			return True
		reply_ntiid = to_external_ntiid_oid( the_object.inReplyTo ) if hasattr( the_object, 'inReplyTo' ) else None
		#__traceback_info__ += getattr( the_object, 'inReplyTo' ), reply_ntiid
		if reply_ntiid is not None and reply_ntiid in self._muted_oids: # Raises TypeError if reply_ntiid is None; this could happen
			return True
		refs_ntiids = [to_external_ntiid_oid(x) for x in the_object.references] if hasattr( the_object, 'references') else ()
		for x in refs_ntiids:
			if x and x in self._muted_oids:
				return True

		# Ok, still going. Walk up through the containerId of this object and its parents, if any,
		# to see if one of them is muted.
		parent = the_object
		while parent is not None:
			if getattr( parent, 'containerId', None ) in self._muted_oids:
				return True
			parent = getattr( parent, '__parent__', None )

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
		wref = nti_interfaces.IWeakRef( source )
		_remove_entity_from_named_lazy_set_of_wrefs( self, '_entities_not_accepted', wref )
		self._entities_accepted.add( wref )
		return True

	def stop_accepting_shared_data_from( self, source ):
		if not source:
			return False
		_remove_entity_from_named_lazy_set_of_wrefs( self, '_entities_accepted', source )
		return True

	@property
	#@deprecate("Prefer `entities_accepting_shared_data_from`")
	def accepting_shared_data_from( self ):
		""" :returns: Iterable names of entities we accept shared data from. """
		return _set_of_usernames_from_named_lazy_set_of_wrefs( self, '_entities_accepted' )

	@property
	def entities_accepting_shared_data_from(self):
		"""
		Return an iterable of entities we accept shared data from.
		"""
		return _iterable_of_entities_from_named_lazy_set_of_wrefs( self, '_entities_accepted' )

	def ignore_shared_data_from( self, source ):
		"""
		The opposite of :meth:`accept_shared_data_from`.

		This method is usually called on the object on behalf of this
		object (e.g., by the user this object represents).
		"""
		if not source:
			return False
		wref = nti_interfaces.IWeakRef( source )
		_remove_entity_from_named_lazy_set_of_wrefs( self, '_entities_accepted', wref )
		self._entities_not_accepted.add( wref )
		return True

	def stop_ignoring_shared_data_from( self, source ):
		if not source:
			return False
		_remove_entity_from_named_lazy_set_of_wrefs( self, '_entities_not_accepted', source )
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
		wref = nti_interfaces.IWeakRef( source )
		for k in ("_entities_accepted", '_entities_not_accepted' ):
			_remove_entity_from_named_lazy_set_of_wrefs( self, k, wref )

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
		if '_entities_not_accepted' in self.__dict__:
			self._entities_not_accepted.clear()

	def reset_accepted_shared_data( self ):
		"""
		Causes this object to forget all accepted users.
		"""
		if '_entities_accepted' in self.__dict__:
			self._entities_accepted.clear()

	@property
	#@deprecate("Prefer `entities_ignoring_shared_data_from`")
	def ignoring_shared_data_from( self ):
		""" :returns: Iterable of names of entities we are specifically ignoring shared data from. """
		return _set_of_usernames_from_named_lazy_set_of_wrefs( self, '_entities_not_accepted' )

	@property
	def entities_ignoring_shared_data_from( self ):
		"""
		Returns an iterable of entities we are specifically ignoring shared data from.
		"""
		return _iterable_of_entities_from_named_lazy_set_of_wrefs( self, '_entities_not_accepted' )

	def is_accepting_shared_data_from( self, source ):
		"""
		Return if this object is accepting data that is explicitly
		shared with it by `source`.
		"""
		# The string path is deprecated
		return source in self.entities_accepting_shared_data_from or (isinstance(source, six.string_types) and source in self.accepting_shared_data_from)

	def is_ignoring_shared_data_from( self, source ):
		"""
		The opposite of :meth:`is_accepting_shared_data_from`
		"""
		# Naturally we ignore ourself
		if source is self or self.username == source:
			return True
		return source in self.entities_ignoring_shared_data_from or (isinstance( source, six.string_types) and source in self.ignoring_shared_data_from)


	# TODO: In addition to the actual explicitly shared objects that I've
	# accepted because I'm not ignoring, we need the "incoming" group
	# for things I haven't yet accepted but are still shared with me.
	def getSharedContainer( self, containerId, defaultValue=() ):
		"""
		Get the shared container.

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
			# Drop the logging to TRACE because at least one of these will be missing.
			# We may get a ContainedObjectValueError if the object was not even sharable to begin with
			try:
				result = containers.deleteEqualContainedObject( contained, log_level=loglevels.TRACE ) or result
			except ValueError:
				pass
		return result

	def _addToStream( self, change ):
		"""
		:return: A boolean indicating whether the change was accepted
		or muted.
		"""
		__traceback_info__ = self
		if self.is_muted( change.object ):
			return False

		self.streamCache.addContainedObject( change )
		return True

	def _removeFromStream( self, change ):
		self.streamCache.deleteEqualContainedObject( change.object )

	def _get_stream_cache_containers( self, containerId ):
		""" Return a sequence of stream cache containers for the id. """
		return (self.streamCache.getContainer( containerId, () ),)

	def getContainedStream( self, containerId, minAge=-1, maxCount=MAX_STREAM_SIZE ):
		# The contained stream is an amalgamation of the traffic explicitly
		# to us, plus the traffic of things we're following. We merge these together and return
		# just the ones that fit the criteria.
		# TODO: What's the right heuristic here? Seems like things shared directly with me
		# may be more important than things I'm following, but only before some age cutoff. For the
		# moment, we are actually merging all this activity together, regardless of source
		# TODO: These data structures could and should be optimized for this.
		result = datastructures.LastModifiedCopyingUserList()

		stream_containers = self._get_stream_cache_containers( containerId )

		change_objects = set()
		def add( item, lm=None ):
			lm = lm or item.lastModified
			result.append( item )
			result.updateLastModIfGreater( lm )
			change_objects.add( item.object )

		def dup( change_object ):
			return change_object in change_objects

		def _make_largest_container( container, of_size, extra_pred=None ):
			container = (item for item in container
						 if item and item.lastModified > minAge
						 and not self.is_ignoring_shared_data_from( item.creator )
						 and (extra_pred is None or extra_pred(item)))
			# Now take the "largest" (newest) of those, sorted by modification
			# (We actually take more than the stream size, to try to ensure that we
			# can fulfill the request)
			container = heapq.nlargest( of_size, container, key=lambda i: i.lastModified )
			return container

		# First, get the N largest of all the containers, and then
		most_recent_in_containers = []
		for stream_container in stream_containers:
			# If the container is potentially larger than the stream size,
			# we want to take only its most recent items, and then only the ones that
			# match our other criteria
			# Start by defining a generator to extract items that match

			# Now take the largest of those, sorted by modification
			# (We actually take more than the stream size, to try to ensure that we
			# can fulfill the request)
			# We sadly have to again apply muting here so that things in the community
			# caches get our muting filters applied to them
			container = _make_largest_container( stream_container, maxCount * 2, lambda x: not self.is_muted(x.object) )

			# In order to heapq.merge, these have to be smallest to largest
			container.reverse()
			most_recent_in_containers.append( ((item.lastModified, item) for item in container) )

		for _, item in reversed(list(heapq.merge(*most_recent_in_containers))):
			if not dup( item.object ):
				add( item )

				if len( result ) >= maxCount:
					break

		if len( result ) >= maxCount:
			return result


		# If we get here, then we weren't able to satisfy the request from the caches. Must walk
		# through the shared items directly.
		# We should probably be able to satisfy the request from the people we
		# follow. If not, we try to fill in with everything shared with us/followed by us
		# being careful to avoid duplicating things present in the stream
		# TODO: We've lost change information for these items.
		extras_needed = maxCount - len(result)
		for item in _make_largest_container( self.getSharedContainer( containerId ), extras_needed, lambda x: not dup(x) ):
			change = Change( Change.SHARED, item )
			change.creator = item.creator or self

			# Since we're fabricating a change for this item,
			# we know it can be no later than when the item itself was last changed
			change.lastModified = item.lastModified

			add( change, item.lastModified )

			if len(result) >= maxCount:
				break

		# Well we've done the best that we can.
		return result

	def _acceptIncomingChange( self, change, direct=True ):
		"""
		:keyword bool direct: If ``True`` (the default) then this change
			is directly targeted toward this object. If false, then the change
			is indirect and was targeted toward some group or community this object
			is a member of. This method only adds shared objects for direct targets;
			all changes are added to the stream.
		:return: A value indicating if the change was actually accepted or
			is muted.
		"""
		accepted = self._addToStream( change )

		if direct and change.is_object_shareable():
			# TODO: What's the right check here?
			self._addSharedObject( change.object )
		return accepted

	def _noticeChange( self, change, force=False ):
		""" Should run in a transaction. """
		# We hope to only get changes for objects shared with us, but
		# we double check to be sure--force causes us to take incoming
		# creations/shares anyway. DELETES must always go through, regardless

		if change.type in (Change.CREATED,Change.SHARED):
			if change.object is not None and self.is_accepting_shared_data_from( change.creator ):
				if change.object.isSharedDirectlyWith( self ):
					self._acceptIncomingChange( change )
				elif change.object.isSharedIndirectlyWith( self ) or force:
					self._acceptIncomingChange( change, direct=False )
		elif change.type == Change.MODIFIED:
			if change.object is not None:
				if change.object.isSharedDirectlyWith( self ):
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
				elif change.object.isSharedIndirectlyWith( self ) or force:
					self._addToStream( change )
				else:
					# FIXME: Badly linear
					self._removeSharedObject( change.object )
		elif change.type == Change.DELETED:
			# The weak refs would clear eventually.
			# For speedy deletion at the expense of scale, we
			# can force the matter
			removed = self._removeSharedObject( change.object )
			if removed is False or removed is None and change.is_object_shareable(): # Explicit, not falsey
				logger.warn( "Incoming deletion (%s) didn't find a shared object in %s", change, self )
			# Hmm. We also feel like we want to remove the entire thing from the stream
			# as well, erasing all evidence that it ever
			# existed
			self._removeFromStream( change )
		elif change.type == Change.CIRCLED:
			self._acceptIncomingChange( change )
		# Do a dual-dispatch to notify complex subscribers that need to know
		# the destination user
		component.handle( self, change )


class SharingSourceMixin(SharingTargetMixin):
	"""
	Something that can share data. These objects are typically
	"active."
	"""

	def __init__( self, *args, **kwargs ):
		super(SharingSourceMixin,self).__init__( *args, **kwargs )

	@Lazy
	def _entities_followed(self):
		"""
		Set of weak-refs to entities we want to follow.
		For users, we will source data specifically
		from them out of communities we belong to. For communities, we will
		take all data (with the exception of _entities_not_accepted, of course.
		"""
		return self._lazy_create_ootreeset_for_wref()

	@Lazy
	def _dynamic_memberships(self):
		"""
		Set of weak-refs to things that offer dynamic
		membership properties for sharing: we can leave and join these
		at any time. Typically they will be ICommunity objects but they
		must be IDynamicSharingTarget objects.
		"""
		return self._lazy_create_ootreeset_for_wref()

	def follow( self, source ):
		""" Adds `source` to the list of followers. """
		self._entities_followed.add( nti_interfaces.IWeakRef(source) )
		return True

	def stop_following( self, source ):
		"""
		Ensures that `source` is no longer in the list of followers.
		"""
		_remove_entity_from_named_lazy_set_of_wrefs( self, '_entities_followed', source )

	@property
	@deprecate("Prefer `entities_followed`")
	def following(self):
		""" :returns: Iterable names of entities we are following. """
		return _set_of_usernames_from_named_lazy_set_of_wrefs( self, '_entities_followed' )

	@property
	def entities_followed(self):
		"""
		Iterable of entities we are following.
		"""
		return _iterable_of_entities_from_named_lazy_set_of_wrefs( self, '_entities_followed' )

	@deprecate("Prefer the `record_dynamic_membership` method.")
	def join_community( self, community ):
		""" Marks this object as a member of `community.` Does not follow `community`.
		:returns: Whether we are now a member of the community. """
		self.record_dynamic_membership( community )
		return True

	def record_dynamic_membership(self, dynamic_sharing_target ):
		"""
		Records the fact that this object is a member of the given dynamic sharing target.
		:param dynamic_sharing_target: The target. Must implement :class:`nti_interfaces.IDynamicSharingTarget`.
		"""
		assert nti_interfaces.IDynamicSharingTarget.providedBy( dynamic_sharing_target )
		self._dynamic_memberships.add( nti_interfaces.IWeakRef( dynamic_sharing_target ) )

	def record_no_longer_dynamic_member( self, dynamic_sharing_target ):
		"""
		Records the fact that this object is no longer a member of the given
		dynamic sharing target.

		:param dynamic_sharing_target: The target. Must implement :class:`nti_interfaces.IDynamicSharingTarget`.
		"""
		assert nti_interfaces.IDynamicSharingTarget.providedBy( dynamic_sharing_target )
		_remove_entity_from_named_lazy_set_of_wrefs( self, '_dynamic_memberships', dynamic_sharing_target )

	@property
	@deprecate("Prefer `dynamic_memberships` or `usernames_of_dynamic_memberships`")
	def communities(self):
		return self.usernames_of_dynamic_memberships

	@property
	def dynamic_memberships(self):
		"""
		An iterable of :class:`nti_interfaces.IDynamicSharingTarget` that we are members of.
		"""
		return _iterable_of_entities_from_named_lazy_set_of_wrefs( self, '_dynamic_memberships' )

	@property
	def usernames_of_dynamic_memberships( self ):
		""" :returns: Iterable names of dynamic sharing targets we belong to. """
		return _set_of_usernames_from_named_lazy_set_of_wrefs( self, '_dynamic_memberships' )

	def _get_dynamic_sharing_targets_for_read( self ):
		return _iterable_of_entities_from_named_lazy_set_of_wrefs( self, '_dynamic_memberships' )

	def _get_entities_followed_for_read( self ):
		return _iterable_of_entities_from_named_lazy_set_of_wrefs( self, '_entities_followed' )

	def _get_stream_cache_containers( self, containerId ):
		# start with ours. This ensures things targeted toward us
		# have the highest chance of making it in the cap if we go in order.
		result = [self.streamCache.getContainer( containerId, () )]

		# add everything we follow. If it's a community, we take the
		# whole thing (ignores are filtered in the parent method). If
		# it's a person, we take stuff they've shared to communities
		# we're a member of

		persons_following = set()
		for following in self._get_entities_followed_for_read():
			if nti_interfaces.IDynamicSharingTarget.providedBy( following ):
				# TODO: Better interface
				result += following._get_stream_cache_containers( containerId )
			else:
				persons_following.add( following )

		for comm in self._get_dynamic_sharing_targets_for_read():
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

		persons_following = set()
		communities_seen = set()
		for following in self._get_entities_followed_for_read():
			if nti_interfaces.IDynamicSharingTarget.providedBy( following ):
				communities_seen.add( following )
				for x in following.getSharedContainer( containerId ):
					try:
						if x is not None and not self.is_ignoring_shared_data_from( x.creator ):
							result.append( x )
							result.updateLastModIfGreater( x.lastModified )
					except POSKeyError: # pragma: no cover
						# an object gone missing. This is bad. NOTE: it may be something nested in x
						logger.warning( "Shared object (%s) missing in '%s' from '%s' to '%s'", type(x), containerId,  following, self )
			else:
				persons_following.add( following )


		for comm in self._get_dynamic_sharing_targets_for_read():
			if comm in communities_seen:
				continue
			for x in comm.getSharedContainer( containerId ):
				try:
					if x and x.creator in persons_following:
						result.append( x )
						result.updateLastModIfGreater( x.lastModified )
				except POSKeyError: # pragma: no cover
					# an object gone missing. This is bad. NOTE: it may be something nested in x
					logger.warning( "Shared object (%s) missing in '%s' dynamically shared from '%s' to '%s'", type(x), containerId, comm, self )

		# If we made no modifications, return the default
		# (which would have already been returned by super; possibly it returned other data)
		if not result:
			return super_result
		return result

import zope.intid.interfaces
@component.adapter(nti_interfaces.IDynamicSharingTarget, zope.intid.interfaces.IIntIdRemovedEvent)
def SharingSourceMixin_dynamicsharingtargetdeleted( target, event ):
	"""
	Look for things that people could have dynamic memberships recorded
	with, and clear them when deleted.
	"""
	# Note: this is on the intid removal, not ObjectRemoved, because
	# by ObjectRemoved time we can't get the intid to match weak refs with

	# This really only matters for IFriendsLists
	# (ICommunity is the only other IDynamicSharingTarget and they don't get deleted)
	if nti_interfaces.IFriendsList.providedBy( target ):
		for entity in target:
			record_no_longer_dynamic_member = getattr( entity, 'record_no_longer_dynamic_member', None )
			if callable(record_no_longer_dynamic_member):
				record_no_longer_dynamic_member( target )
				entity.stop_following( target )


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

class AbstractReadableSharedMixin(object):
	"""
	A mixin for implementing :class:`.IReadableShared`. This class defines everything
	in terms of the ``sharingTargets`` property; subclasses simply need to provide
	an implementation for that property. (For optional efficiency, subclasses can override
	:meth:`_may_have_sharing_targets`)

	"""

	def __init__( self ):
		super(AbstractReadableSharedMixin,self).__init__()

	def _may_have_sharing_targets( self ):
		"""
		Called in the implementation of the public query methods to aid efficiency. If this
		returns false, then some algorithms may be short-circuited.
		"""

		return  hasattr( self, 'sharingTargets' ) # always assume the worst, so long as the property exists

	def isSharedDirectlyWith( self, wants ):
		"""
		Checks if we are directly shared with `wants`, which must be a
		Principal.
		"""
		if not self._may_have_sharing_targets( ):
			return False

		try:
			return wants in self.sharingTargets
		except KeyError:
			pass

	def isSharedIndirectlyWith( self, wants ):
		"""
		Checks if we are indirectly shared with `wants` (a Principal).
		"""

		if not self._may_have_sharing_targets():
			return False

		for target in self.sharingTargets:
			for entity in nti_interfaces.IEntityIterable( target, () ):
				if entity == wants:
					return True

	def isSharedWith( self, wants ):
		"""
		Checks if we are directly or indirectly shared with `wants` (a principal)
		"""
		return self.isSharedDirectlyWith( wants ) or self.isSharedIndirectlyWith( wants )

	@property
	def flattenedSharingTargets( self ):
		"""
		A flattened :class:`set` of entities with whom this item is shared.
		"""
		if not self._may_have_sharing_targets():
			return set()

		result = set()
		for x in self.sharingTargets:
			result.add( x ) # always this one
			# then expand if needed
			iterable = nti_interfaces.IEntityIterable( x, None )
			if iterable is not None:
				result.update( iterable )

		return result

	def getFlattenedSharingTargetNames(self):
		"""
		Returns a flattened :class:`set` of :class:`SharingTarget` usernames with whom this item
		is shared.
		"""
		return {x.username for x in self.flattenedSharingTargets}

	# It would be nice to use CachedProperty here, but it doesn't quite play right with
	# object-values for dependent keys
	flattenedSharingTargetNames = property(getFlattenedSharingTargetNames)

from nti.externalization.externalization import to_external_object
class AbstractReadableSharedWithMixin(AbstractReadableSharedMixin):
	"""
	Extends :class:`AbstractReadableSharedMixin` to provide the :meth:`sharedWith` property.
	"""

	@property
	def sharedWith(self):
		"""
		Return the string usernames of things we're sharing with. Note that instead of
		just using the 'username' attribute directly ourself, we are externalizing
		the whole object and returning the externalized value of the username.
		This lets us be consistent with any cases where we are playing games
		with the external value of the username, such as with DynamicFriendsLists.
		"""
		ext_shared_with = []
		for entity in self.sharingTargets:
			# NOTE: This entire process does way too much work for as often as this
			# is called so we hack this and couple it tightly to when we think
			# we need to use it. See nti.appserver._adapters
			#ext_shared_with.append( toExternalObject( entity )['Username'] )
			if nti_interfaces.IDynamicSharingTargetFriendsList.providedBy( entity ):
				username = entity.NTIID
			elif nti_interfaces.IUser.providedBy( entity ) or nti_interfaces.ICommunity.providedBy( entity ):
				username = entity.username
			else:
				# Hmm, what do we have here?
				username = to_external_object( entity )['Username']

			ext_shared_with.append( username )
		return set(ext_shared_with)

from nti.dataserver.authentication import _dynamic_memberships_that_participate_in_security
from nti.dataserver.traversal import find_interface

class AbstractDefaultPublishableSharedWithMixin(AbstractReadableSharedWithMixin):
	"""
	Base class that implements ``sharingTargets`` and ``sharedWith``
	through the presence of the :class:`.IDefaultPublished` interface.
	We say that all instances that are published are shared with all
	the dynamic memberships of the creator or owner of the object (either
	the creator attribute, or the first :class:`.IUser` in our lineage)

	"""

	def _may_have_sharing_targets( self ):
		return nti_interfaces.IDefaultPublished.providedBy( self )

	@property
	def sharingTargets(self):
		if nti_interfaces.IDefaultPublished.providedBy( self ):
			return self.sharingTargetsWhenPublished
		return ()

	@property
	def sharingTargetsWhenPublished(self):
		creator = getattr( self, 'creator', None )
		# interestingly, IUser does not extend IPrincipal
		owner = creator if nti_interfaces.IUser.providedBy( creator ) else find_interface( self, nti_interfaces.IUser )
		# TODO: Using a private function
		# This returns a generator, the schema says we need a 'UniqueIterable'
		return _dynamic_memberships_that_participate_in_security( owner, as_principals=False )


def _ii_family():
	intids = component.queryUtility( zc_intid.IIntIds )
	if intids:
		return intids.family
	return BTrees.family64

@interface.implementer(nti_interfaces.IWritableShared)
class ShareableMixin(AbstractReadableSharedWithMixin, datastructures.CreatedModDateTrackingObject):
	""" Represents something that can be shared. It has a set of SharingTargets
	with which it is shared (permissions) and some flags. Only its creator
	can alter its sharing targets. It may be possible to copy this object. """

	# An IITreeSet of string userids
	# TODO: FIXME: When the user is deleted and his ID goes bad, we're
	# not listening for that. What if the ID gets reused for something else?
	_sharingTargets = None

	def __init__( self ):
		super(ShareableMixin,self).__init__()

	def _may_have_sharing_targets(self):
		return bool(self._sharingTargets)

	def clearSharingTargets( self ):
		if self._sharingTargets is not None:
			self._sharingTargets.clear() # Preserve existing object

			self.updateLastMod()

	def addSharingTarget( self, target ):
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
				self.addSharingTarget( t )
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

		:param replacement_targets: A collection of entities to share with.
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


	def isSharedDirectlyWith( self, wants ):
		"""
		Checks if we are directly shared with `wants`, which must be a
		Principal.
		"""
		if not self._may_have_sharing_targets():
			return False

		# We can be slightly more efficient than the superclass
		try:
			return _getId( wants ) in self._sharingTargets
		except KeyError:
			pass

	@property
	def sharingTargets(self):
		"""
		Returns a simple :class:`set` of entities with whom this item
		is shared.
		"""
		if self._sharingTargets is None:
			return set()
		# Provide a bit of defense against the intids going away or changing
		# out from under us
		return set( (x for x in IntidResolvingIterable( self._sharingTargets, allow_missing=True, parent=self, name='sharingTargets' )
					if x is not None and hasattr( x, 'username') ) )
