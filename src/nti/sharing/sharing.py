#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Classes related to managing the sharing process.

.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import six
import heapq
import collections

heapq_heappush = heapq.heappush
heapq_heappushpop = heapq.heappushpop

import BTrees
from BTrees.OOBTree import OOTreeSet

import persistent

from ZODB import loglevels

from ZODB.POSException import POSKeyError

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.container.contained import Contained

from zope.deprecation import deprecate

from zope.intid.interfaces import IIntIds

from zope.location import locate

from zope.event import notify as _znotify

from zope.security.interfaces import IPrincipal

from nti.common import sets

from nti.containers.datastructures import IntidContainedStorage
from nti.containers.datastructures import IntidResolvingIterable

from nti.dataserver.activitystream_change import Change

from nti.dataserver.interfaces import IUser
from nti.dataserver.interfaces import ICommunity
from nti.dataserver.interfaces import IFriendsList
from nti.dataserver.interfaces import IMutedInStream
from nti.dataserver.interfaces import IWritableShared
from nti.dataserver.interfaces import IEntityContainer
from nti.dataserver.interfaces import StopFollowingEvent
from nti.dataserver.interfaces import FollowerAddedEvent
from nti.dataserver.interfaces import EntityFollowingEvent
from nti.dataserver.interfaces import IDynamicSharingTarget
from nti.dataserver.interfaces import IDeletedObjectPlaceholder
from nti.dataserver.interfaces import ObjectSharingModifiedEvent
from nti.dataserver.interfaces import StopDynamicMembershipEvent
from nti.dataserver.interfaces import StartDynamicMembershipEvent
from nti.dataserver.interfaces import IUseNTIIDAsExternalUsername
from nti.dataserver.interfaces import ISharingTargetEntityIterable

from nti.datastructures.datastructures import LastModifiedCopyingUserList
from nti.datastructures.datastructures import check_contained_object_for_storage

from nti.dublincore.datastructures import CreatedModDateTrackingObject

from nti.ntiids.oids import to_external_ntiid_oid

from nti.publishing.interfaces import IDefaultPublished

from nti.wref.interfaces import IWeakRef

from nti.zodb.containers import time_to_64bit_int as _time_to_64bit_int

_marker = object()

logger = __import__('logging').getLogger(__name__)

# TODO: This all needs refactored. The different pieces need to be broken into
# different interfaces and adapters, probably using annotations, to get most
# of this out of the core object structure, and to make more things possible.
def _getId(contained, when_none=_marker):
    if contained is None and when_none is not _marker:
        return when_none
    return component.getUtility(IIntIds).getId(contained)


class _SharedContainedObjectStorage(IntidContainedStorage):
    """
    An object that implements something like the interface of
    :class:`datastructures.ContainedStorage`, but in a simpler form using only intids,
    and assuming that we never need to look objects up by container/localID pairs.
    """
    # This class exists for backwards compatibilty in pickles, and
    # to override the contained object check
    def _check_contained_object_for_storage(self, contained):
        check_contained_object_for_storage(contained)


class _SharedStreamCache(persistent.Persistent, Contained):
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

    def __init__(self, family=None):
        super(_SharedStreamCache, self).__init__()
        if family is not None:  # pragma: no cover
            self.family = family
        else:
            intids = component.queryUtility(IIntIds)
            if intids is not None and intids.family != self.family:
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

    def _read_current(self, container):
        try:
            # it is important to activate the object before we read current on it,
            # so that we get a non-zero serial to record in the transaction.
            # usually the object has already been activated, but in our case we fetch from
            # a map and then immediately call read_current so it is probably
            # not active.
            # pylint: disable=protected-access
            container._p_activate()
            container._p_jar.readCurrent(container)
        except AttributeError:  # pragma: no cover
            pass

    # We use -1 as values for None. This is common in test cases
    # and possibly for deleted objects (there can only be one of these)

    def addContainedObject(self, change):
        # We used to raise if None here; should be safe to default to empty
        # string.
        change_containerId = change.containerId or u''
        for _containers, factory in ((self._containers_modified, self.family.II.BTree),
                                     (self._containers, self.family.IO.BTree)):
            self._read_current(_containers)
            container_map = _containers.get(change_containerId)
            self._read_current(container_map)
            if container_map is None:
                container_map = factory()
                _containers[change_containerId] = container_map

        # And so at this point, `container_map` is an IOBTree
        obj_id = _getId(change.object, -1)
        old_change = container_map.get(obj_id)
        container_map[obj_id] = change

        # Now save the modification info.
        # Note that container_map is basically a set on change.object, but
        # we might actually get many different changes for a given object.
        # that's why we have to get the one we're replacing (if any)
        # and remove that timestamp from the modified map
        modified_map = self._containers_modified[change_containerId]

        if old_change is not None:
            modified_map.pop(_time_to_64bit_int(old_change.lastModified), None)

        modified_map[_time_to_64bit_int(change.lastModified)] = obj_id

        # If we're too big, start trimming
        while len(modified_map) > self.stream_cache_size:
            oldest_id = modified_map.pop(modified_map.minKey())
            # If this pop fails, we are somehow corrupted, in that our state
            # doesn't match. It's a relatively minor corruption, however,
            # (the worst that happens is that the stream cache gets a bit too big) so
            # the most it entails is logging.
            # In the future we may need to rebuild datastructures that exhibit
            # this problem. their stream may need to be cleared
            try:
                container_map.pop(oldest_id)
            except KeyError:  # pragma: no cover
                logger.debug("Failed to pop oldest object with id %s in %s",
                             oldest_id, self)

        # Change objects can wind up sent to multiple people in different shards
        # They need to have an owning shard, otherwise it's not possible to pick
        # one if they are reachable from multiple. So we add them here
        # TODO: See comments above about making the user own these
        if self._p_jar and getattr(change, '_p_jar', self) is None:
            self._p_jar.add(change)

        return change

    def deleteEqualContainedObject(self, contained, log_level=None):
        # pylint: disable=unused-variable
        __traceback_info__ = contained, log_level
        # It is important to ensure we are reading the current, consistent
        # state of objects, as pop() does not mark the object modified if it
        # is not found (but some other transaction could have added it while we were
        # running, and we wouldn't get a conflict)
        self._read_current(self._containers_modified)
        obj_id = _getId(contained)
        containerId = contained.containerId or ''  # Bucket into empty if None

        modified_map = self._containers_modified.get(containerId)
        if modified_map is not None:
            self._read_current(modified_map)
            modified_map.pop(_time_to_64bit_int(contained.lastModified), None)

        self._read_current(self._containers)
        container_map = self._containers.get(containerId)
        if container_map is not None:
            self._read_current(container_map)
            if container_map.pop(obj_id, None) is not None:
                return contained

    def clearContainer(self, containerId):
        self._containers.pop(containerId, None)
        self._containers_modified.pop(containerId, None)

    def clear(self):
        self._containers.clear()
        self._containers_modified.clear()

    def getContainer(self, containerId, defaultValue=None):
        """
        Returns something that can iterate across Change objects, or the default value.
        The returned object also has a ``iter_between`` method that accepts up to two `time.time`
        values (min,max);. If min is given and not none, that will be the lower bound timestamp
        for changes; if max is given and not none, that will be the upper bound timestamp (if max
        is greater than min, no changes are returned). A value of None for either
        boundary means no limit. This can be used to efficiently combine
        streams, picking up only newer items.
        """
        containerId = containerId or ''  # Bucket into empty if None
        container = self._containers.get(containerId)
        if container is None:
            return defaultValue
        mod_container = self._containers_modified.get(containerId)
        if mod_container is None:  # pragma: no cover
            logger.warning("Corruption detected in %s", self)
            return container.values()
        # TODO: If needed, we could get a 'Last Modified' value for
        # this returned object using self._containers_modified
        return _StreamValuesProxy(container, mod_container)

    def values(self):
        # Iter the keys and call getContainer to get wrapping
        for k in self._containers:
            # FIXME: What? There's no wrapping anymore
            yield self.getContainer(k)

    def iteritems(self):
        for k in self._containers:
            yield (k, self.getContainer(k))

    def keys(self):
        return self._containers.keys()

    def __iter__(self):
        return iter(self._containers)

    def __repr__(self):
        return '<%s at %s/%s>' % (self.__class__.__name__, self.__parent__, self.__name__)


class _StreamValuesProxy(object):

    __slots__ = ('_container', '_mod_container')

    def __init__(self, container, mod_container):
        self._container = container
        self._mod_container = mod_container

    def __iter__(self):
        return iter(self._container.values())

    def __len__(self):
        return len(self._container)  # VERY inefficient

    def iter_between(self, min_age, max_age):
        min_time_key = _time_to_64bit_int(min_age) if min_age else None
        max_time_key = _time_to_64bit_int(max_age) if max_age else None
        container = self._container
        for obj_id in self._mod_container.values(min_time_key, max_time_key):
            change = container.get(obj_id)
            if change is not None:
                yield change


def _set_of_usernames_from_named_lazy_set_of_wrefs(obj, name):
    result = set()
    container = _iterable_of_entities_from_named_lazy_set_of_wrefs(obj, name)
    for entity in container:
        if IUseNTIIDAsExternalUsername.providedBy(entity):
            result.add(entity.NTIID)
        else:
            result.add(entity.username)
    return result


def _iterable_of_entities_from_named_lazy_set_of_wrefs(obj, name):
    container = ()
    # pylint: disable=protected-access
    obj._p_activate()  # Ensure we have a dict
    if name in obj.__dict__:
        container = getattr(obj, name)
    for wref in container:
        val = wref(allow_cached=False)
        if val is not None:
            yield val


def _remove_entity_from_named_lazy_set_of_wrefs(self, name, entity):
    # pylint: disable=protected-access
    self._p_activate()  # Ensure we have a dict
    if name in self.__dict__:
        jar = getattr(self, '_p_jar', None)
        container = getattr(self, name)
        if jar is not None:
            # Since we're mutating, probably based on some other object's
            # content, make sure we're mutating the current version
            jar.readCurrent(self)
            container._p_activate()
            jar.readCurrent(container)
        wref = IWeakRef(entity)
        # pylint: disable=unused-variable
        __traceback_info__ = entity, wref
        assert hasattr(wref, 'username')
        sets.discard(container, wref)


class _SharingContextCache(object):
    """
    Provisional API to enable some caching to happen
    when looking through multiple containers/streams.
    Outside of this module, this is an opaque object.
    """

    def __init__(self):
        self._data = {}
        self._dups = set()
        self._accumulator = None
        self.lastModified = 0
        self.communities_followed = None
        self.persons_followed = None

    def updateLastModIfGreater(self, t):
        self.lastModified = max(self.lastModified, t)

    def make_accumulator(self):
        self._accumulator = LastModifiedCopyingUserList()

    def get_accumulator(self):
        return self._accumulator

    def to_result(self, accumulator=None):
        """
        Destroys accumulator
        """
        if accumulator is None:
            accumulator = self._accumulator

        result = LastModifiedCopyingUserList()
        # must sort the accumulator, not the change objects;
        # change objects have arbitrary comparison
        accumulator.sort(reverse=True)  # Newest first
        result.extend((x[1] for x in accumulator))
        if result:
            result.updateLastModIfGreater(result[0].lastModified)
        return result

    # To avoid duplicates, we keep a set of the OIDs/intids of the
    # objects. We use this rather than the object itself for speed,
    # (hashing the objects here showed up as a hotspot in profiling)
    # and to ensure mutual hash/equal works
    def _has_seen_object(self, obj):
        return id(obj) in self._dups

    def _note_seen_object(self, obj):
        self._dups.add(id(obj))

    def _build_entities_followed_for_read(self, entity):
        if self.communities_followed is not None:
            return
        communities_followed = self.communities_followed = []
        persons_followed = self.persons_followed = []

        # pylint: disable=protected-access
        for following in self(entity._get_entities_followed_for_read):
            if IDynamicSharingTarget.providedBy(following):
                communities_followed.append(following)
            else:
                persons_followed.append(following)

    def __call__(self, func):
        # makes many assumptions. Func must return an iterable that we
        # transform into a set
        key = func.__name__
        if key in self._data:
            return self._data[key]

        result = list(func())
        self._data[key] = result
        return result


SharingContextCache = _SharingContextCache


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

    def __init__(self, *args, **kwargs):  # pylint: disable=useless-super-delegation
        super(SharingTargetMixin, self).__init__(*args, **kwargs)

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
            self._p_jar.add(cache)
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
            self._p_jar.add(result)
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
            self._p_jar.add(result)
        locate(result, self, 'containers_of_muted')
        return result

    def _lazy_create_ootreeset_for_wref(self):
        self._p_changed = True
        result = OOTreeSet()
        if self._p_jar:
            self._p_jar.add(result)
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

    def __manage_mute(self, mute=True):
        # TODO: Horribly inefficient
        # pylint: disable=no-member,protected-access
        if self._p_jar and self.containersOfShared._p_jar:
            self.containersOfShared._p_activate()
            self.containers_of_muted._p_activate()
            self._p_jar.readCurrent(self.containersOfShared)
            self._p_jar.readCurrent(self.containers_of_muted)
        _from = self.containersOfShared
        _to = self.containers_of_muted
        if not mute:
            _from, _to = _to, _from

        to_move = []
        for container in _from.containers.values():
            for obj in container:
                if mute:
                    if self.is_muted(obj):
                        to_move.append(obj)
                elif not self.is_muted(obj):
                    to_move.append(obj)

        for x in to_move:
            _from.deleteEqualContainedObject(x)
            _to.addContainedObject(x)

            if mute:
                self.streamCache.deleteEqualContainedObject(x)

        # There is the possibility for things to be in the stream
        # cache that were never in the shared objects (forums)
        # However, we have to apply muting at read time anyway, so
        # there's no point going through the containers now

    def mute_conversation(self, root_ntiid_oid):
        """
        :raises TypeError: If `root_ntiid_oid` is ``None``.
        """
        # self._muted_oids would raise TypeError, but no need to
        # run the lazy creator if not needed
        if root_ntiid_oid is None:
            # match BTree message
            raise TypeError('Object has default comparison')
        # pylint: disable=no-member,
        self._muted_oids.add(root_ntiid_oid)

        # Now move over anything that is muted
        self.__manage_mute()

    def unmute_conversation(self, root_ntiid_oid):
        if '_muted_oids' not in self.__dict__ or root_ntiid_oid is None:
            # No need to let self._muted_oids raise TypeError if root_ntiid_oid is
            # None: It could never possibly be muted
            return

        if sets.discard_p(self._muted_oids, root_ntiid_oid):
            # Now unmute anything required
            self.__manage_mute(mute=False)

    def is_muted(self, the_object):
        if IMutedInStream.providedBy(the_object):
            return True

        if the_object is None or '_muted_oids' not in self.__dict__:
            return False

        try:
            if (getattr(the_object, 'id', None) or '') in self._muted_oids:
                return True
        except KeyError:  # POSKeyError
            return True
        ntiid = to_external_ntiid_oid(the_object)
        # __traceback_info__ = the_object, ntiid
        if ntiid and ntiid in self._muted_oids:  # Raises TypeError if ntiid is None
            return True
        reply_ntiid = to_external_ntiid_oid(the_object.inReplyTo) if hasattr(
            the_object, 'inReplyTo') else None
        # __traceback_info__ += getattr( the_object, 'inReplyTo' ), reply_ntiid
        # Raises TypeError if reply_ntiid is None; this could happen
        if reply_ntiid is not None and reply_ntiid in self._muted_oids:
            return True
        refs_ntiids = [to_external_ntiid_oid(x) for x in the_object.references] if hasattr(
            the_object, 'references') else ()
        for x in refs_ntiids:
            if x and x in self._muted_oids:
                return True

        # Ok, still going. Walk up through the containerId of this object and its parents, if any,
        # to see if one of them is muted.
        parent = the_object
        while parent is not None:
            # be sure to avoid testing None as above
            if (getattr(parent, 'containerId', None) or '') in self._muted_oids:
                return True
            parent = getattr(parent, '__parent__', None)

        return False

    def accept_shared_data_from(self, source):
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
        wref = IWeakRef(source)
        _remove_entity_from_named_lazy_set_of_wrefs(
            self, '_entities_not_accepted', wref
        )
        # pylint: disable=no-member
        self._entities_accepted.add(wref)
        return True

    def stop_accepting_shared_data_from(self, source):
        if not source:
            return False
        _remove_entity_from_named_lazy_set_of_wrefs(
            self, '_entities_accepted', source)
        return True

    @property
    # @deprecate("Prefer `entities_accepting_shared_data_from`")
    def accepting_shared_data_from(self):
        """ 
        :returns: Iterable names of entities we accept shared data from.
        """
        return _set_of_usernames_from_named_lazy_set_of_wrefs(self, '_entities_accepted')

    @property
    def entities_accepting_shared_data_from(self):
        """
        Return an iterable of entities we accept shared data from.
        """
        return _iterable_of_entities_from_named_lazy_set_of_wrefs(self, '_entities_accepted')

    def ignore_shared_data_from(self, source):
        """
        The opposite of :meth:`accept_shared_data_from`.

        This method is usually called on the object on behalf of this
        object (e.g., by the user this object represents).
        """
        if not source:
            return False
        wref = IWeakRef(source)
        _remove_entity_from_named_lazy_set_of_wrefs(
            self, '_entities_accepted', wref
        )
        # pylint: disable=no-member
        self._entities_not_accepted.add(wref)
        return True

    def stop_ignoring_shared_data_from(self, source):
        if not source:
            return False
        _remove_entity_from_named_lazy_set_of_wrefs(
            self, '_entities_not_accepted', source
        )
        return True

    def reset_shared_data_from(self, source):
        """
        Stop accepting shared data from the `source`, but also do not ignore it.

        This method is usually called on the object on behalf of this
        object.

        :returns: A truth value of whether or not we accepted the
                reset. This implementation returns True if source is valid.
        """
        if not source:
            return False
        wref = IWeakRef(source)
        for k in ("_entities_accepted", '_entities_not_accepted'):
            _remove_entity_from_named_lazy_set_of_wrefs(self, k, wref)

    def reset_all_shared_data(self):
        """
        Causes this object to forget all sharing and ignoring settings.
        """
        # Keep the same objects in case of references
        self.reset_ignored_shared_data()
        self.reset_accepted_shared_data()

    def reset_ignored_shared_data(self):
        """
        Causes this object to forget all ignored settings.
        """
        if '_entities_not_accepted' in self.__dict__:
            # pylint: disable=no-member,
            self._entities_not_accepted.clear()

    def reset_accepted_shared_data(self):
        """
        Causes this object to forget all accepted users.
        """
        if '_entities_accepted' in self.__dict__:
            # pylint: disable=no-member,
            self._entities_accepted.clear()

    @property
    # @deprecate("Prefer `entities_ignoring_shared_data_from`")
    def ignoring_shared_data_from(self):
        """
        :returns: Iterable of names of entities we are specifically ignoring shared data from.
        """
        return _set_of_usernames_from_named_lazy_set_of_wrefs(self, '_entities_not_accepted')

    @property
    def entities_ignoring_shared_data_from(self):
        """
        Returns an iterable of entities we are specifically ignoring shared data from.
        """
        return _iterable_of_entities_from_named_lazy_set_of_wrefs(self, '_entities_not_accepted')

    def is_accepting_shared_data_from(self, source):
        """
        Return if this object is accepting data that is explicitly
        shared with it by `source`.
        """
        # The string path is deprecated
        return source in self.entities_accepting_shared_data_from \
               or (isinstance(source, six.string_types) and source in self.accepting_shared_data_from)

    def is_ignoring_shared_data_from(self, source):
        """
        The opposite of :meth:`is_accepting_shared_data_from`
        """
        # Naturally we ignore ourself
        if source is self or self.username == source:
            return True
        return source in self.entities_ignoring_shared_data_from \
            or (isinstance(source, six.string_types) and source in self.ignoring_shared_data_from)

    # TODO: In addition to the actual explicitly shared objects that I've
    # accepted because I'm not ignoring, we need the "incoming" group
    # for things I haven't yet accepted but are still shared with me.
    def getSharedContainer(self, containerId, defaultValue=(), context_cache=None):
        """
        Get the shared container.

        :return: If the containerId is found, an iterable of callable objects (weak refs);
                calling the objects will either return the actual shared object, or None.
        """
        # pylint: disable=unused-variable
        __traceback_info__ = containerId, defaultValue, context_cache
        # pylint: disable=no-member
        result = self.containersOfShared.getContainer(
            containerId, defaultValue
        )
        return result

    def _addSharedObject(self, contained):
        containers = self.containers_of_muted if self.is_muted(contained) else self.containersOfShared
        containers.addContainedObject(contained)

    def _removeSharedObject(self, contained):
        """
        :return: The removed object, or None if nothing was removed.
        """
        # Remove from both muted and normal, just in case
        result = False
        for containers in (self.containersOfShared, self.containers_of_muted):
            # Drop the logging to TRACE because at least one of these will be missing.
            # We may get a ContainedObjectValueError if the object was not even
            # sharable to begin with
            try:
                # pylint: disable=no-member
                result = containers.deleteEqualContainedObject(contained, log_level=loglevels.TRACE) \
                      or result
            except ValueError:
                pass
        return result

    def _addToStream(self, change):
        """
        :return: A boolean indicating whether the change was accepted or muted.
        """
        # pylint: disable=unused-variable
        __traceback_info__ = self
        if self.is_muted(change.object):
            return False
        # pylint: disable=no-member,
        self.streamCache.addContainedObject(change)
        return True

    def _removeFromStream(self, change):
        # pylint: disable=no-member
        self.streamCache.deleteEqualContainedObject(change.object)

    def _get_stream_cache_containers(self, containerId, context_cache=None):
        """
         Return a sequence of stream cache containers for the id.
         An item can optionally be a tuple of the container and an extra predicate to
         apply to items in that container.
        """
        # pylint: disable=unused-variable
        __traceback_info__ = containerId, context_cache
        # pylint: disable=no-member
        return (self.streamCache.getContainer(containerId, ()),)

    def getContainedStream(self, containerId, minAge=-1, maxCount=MAX_STREAM_SIZE, before=-1,
                           context_cache=None, predicate=None):
        """
        Return the most recent items from the stream. Only changes that do not refer to missing or deleted
        objects are returned. Only changes that were created by extant users are returned.

        :param containerId: Look in this container.
        :keyword maxCount: Return at most this many items.
        :keyword before: A ``time.time`` value; only items older than this timestamp
                will be considered. Use this for paging backwards through time.
        :keyword context_cache: Set this to a :class:`._SharingContextCache` if you will be making multiple
                requests for different containers. Call ``make_accumulator`` on it before you begin,
                and ``to_result`` on it when you are finished. This is the most efficient way to sort
                and page through multiple containers.
        :keyword predicate: Set this to a callable that returns True on items to keep. It will
                be applied after all our built-in logic.
        """
        # pylint: disable=protected-access
        context_cache = context_cache or _SharingContextCache()
        # The contained stream is an amalgamation of the traffic explicitly
        # to us, plus the traffic of things we're following. We merge these together and return
        # just the ones that fit the criteria.
        # TODO: What's the right heuristic here? Seems like things shared directly with me
        # may be more important than things I'm following, but only before some age cutoff. For the
        # moment, we are actually merging all this activity together, regardless of source
        # TODO: These data structures could and should be optimized for this.

        # result = context_cache._accumulator if context_cache._accumulator is not None else
        # We maintain this as a heap, with the newest item at the index 0
        if context_cache.get_accumulator() is None:
            accumulator = []
            result = LastModifiedCopyingUserList()
        else:
            accumulator = context_cache.get_accumulator()
            result = None

        stream_containers = self._get_stream_cache_containers(
            containerId, context_cache=context_cache)

        for stream_container in stream_containers:
            if () == stream_container:
                continue
            _container_predicate = None
            if isinstance(stream_container, tuple):
                _container_predicate = stream_container[1]
                stream_container = stream_container[0]

            if hasattr(stream_container, 'iter_between'):
                minAge = minAge if minAge is not None and minAge > 0 else None
                maxAge = before if before is not None and before > 0 else None
                # Once the heap is full, we can start efficiently looking at only the newer
                # changes (not even loading them from the database), if the container supports it.
                # We can also do this upon request
                if len(accumulator) >= maxCount:
                    minAge = accumulator[0][0]

                stream_container = stream_container.iter_between(
                    minAge, maxAge
                )

            for change in stream_container:
                if change is None:
                    continue
                # If the heap is full, and this item is older than the oldest thing in the
                # heap, no need to look any further
                try:
                    change_lastModified = change.lastModified
                    if ((minAge is not None and change_lastModified < minAge)
                            or (before != -1 and change_lastModified >= before)):
                        continue
                except KeyError:  # POSKeyError
                    logger.warn("POSKeyError in stream %s", containerId)
                    continue

                if len(accumulator) == maxCount and change_lastModified <= accumulator[0][0]:
                    continue

                data = change.object
                if data is None or context_cache._has_seen_object(data):
                    continue

                change_creator = change.creator
                if (not change.creator
                        or self.is_ignoring_shared_data_from(change_creator)
                        or IDeletedObjectPlaceholder.providedBy(data)
                        or self.is_muted(data)):
                    continue

                if _container_predicate is not None and not _container_predicate(change):
                    continue

                # Last, if the user supplied a predicate, try it.
                if predicate is not None and not predicate(change):
                    continue

                # Yay, we got one
                context_cache._note_seen_object(data)
                context_cache.updateLastModIfGreater(change_lastModified)

                heaped = (change_lastModified, change)
                if not accumulator:  # starting out
                    accumulator.append(heaped)
                elif len(accumulator) < maxCount:  # growing the heap
                    heapq_heappush(accumulator, heaped)
                else:  # heap full, we may or may not have something newer
                    heapq_heappushpop(accumulator, heaped)

        if result is not None and accumulator:
            # We aren't accumulating for later, and we found data
            # Put them in newest first
            result = context_cache.to_result(accumulator)
        return result

    def _acceptIncomingChange(self, change, direct=True):
        """
        :keyword bool direct: If ``True`` (the default) then this change
                is directly targeted toward this object. If false, then the change
                is indirect and was targeted toward some group or community this object
                is a member of. This method only adds shared objects for direct targets;
                all changes are added to the stream.
        :return: A value indicating if the change was actually accepted or
                is muted.
        """
        accepted = self._addToStream(change)
        if direct and change.is_object_shareable():
            # TODO: What's the right check here?
            self._addSharedObject(change.object)
        return accepted

    def _noticeChange(self, change, force=False):
        """
        Should run in a transaction.
        """
        # We hope to only get changes for objects shared with us, but
        # we double check to be sure--force causes us to take incoming
        # creations/shares anyway. DELETES must always go through, regardless

        if change.type in (Change.CREATED, Change.SHARED):
            if change.object is not None and self.is_accepting_shared_data_from(change.creator):
                if change.object.isSharedDirectlyWith(self):
                    self._acceptIncomingChange(change)
                elif change.object.isSharedIndirectlyWith(self) or force:
                    self._acceptIncomingChange(change, direct=False)
        elif change.type == Change.MODIFIED:
            if change.object is not None:
                if change.object.isSharedDirectlyWith(self):
                    # TODO: Each change is going into the stream
                    # leading to the possibility of multiple of the same objects
                    # in the stream.
                    # We should NOT have duplicates in the shared objects,
                    # though, because we're maintaining that as a map keyed by
                    # IDs. The container does detect and abort attempts to insert
                    # duplicate keys before the original is removed, so
                    # order matters
                    # Mimic what we do when adding new change.
                    self._addToStream(change)
                    if change.is_object_shareable():
                        self._addSharedObject(change.object)
                elif change.object.isSharedIndirectlyWith(self) or force:
                    self._addToStream(change)
                else:
                    # FIXME: Badly linear
                    self._removeSharedObject(change.object)
        elif change.type == Change.DELETED:
            # The weak refs would clear eventually.
            # For speedy deletion at the expense of scale, we can force the
            # matter.
            removed = self._removeSharedObject(change.object)
            if (removed is False or removed is None) and change.is_object_shareable():  # Explicit, not falsey
                # We expected the item in the shared container, but didn't find
                # it.
                logger.warning("Incoming deletion (%s) didn't find a shared object in %s", 
                               change, self)
            # Hmm. We also feel like we want to remove the entire thing from the stream
            # as well, erasing all evidence that it ever
            # existed
            self._removeFromStream(change)
        elif change.type == Change.CIRCLED:
            self._acceptIncomingChange(change)
        # Do a dual-dispatch to notify complex subscribers that need to know
        # the destination user
        component.handle(self, change)


class SharingSourceMixin(SharingTargetMixin):
    """
    Something that can share data. These objects are typically
    "active."
    """

    def __init__(self, *args, **kwargs):  # pylint: disable=useless-super-delegation
        super(SharingSourceMixin, self).__init__(*args, **kwargs)

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

    def follow(self, source):
        """
        Adds ``source`` to the list of followers.

        If ``source`` is actually added, notifies an :class:`.IEntityFollowingEvent`
        and :class:`.IFollowerAddedEvent`.
        """
        # pylint: disable=no-member
        if self._entities_followed.add(IWeakRef(source)):
            _znotify(EntityFollowingEvent(self, source))
            _znotify(FollowerAddedEvent(source, self))
        return True

    def stop_following(self, source):
        """
        Ensures that `source` is no longer in the list of followers.
        """
        _remove_entity_from_named_lazy_set_of_wrefs(
            self, '_entities_followed', source
        )
        _znotify(StopFollowingEvent(self, source))

    @property
    @deprecate("Prefer `entities_followed`")
    def following(self):
        """ :returns: Iterable names of entities we are following. """
        return _set_of_usernames_from_named_lazy_set_of_wrefs(self, '_entities_followed')

    @property
    def entities_followed(self):
        """
        Iterable of entities we are following.
        """
        return _iterable_of_entities_from_named_lazy_set_of_wrefs(self, '_entities_followed')

    @deprecate("Prefer the `record_dynamic_membership` method.")
    def join_community(self, community):
        """ Marks this object as a member of `community.` Does not follow `community`.
        :returns: Whether we are now a member of the community. """
        self.record_dynamic_membership(community)
        return True

    def record_dynamic_membership(self, dynamic_sharing_target):
        """
        Records the fact that this object is a member of the given dynamic sharing target.
        :param dynamic_sharing_target: The target. Must implement :class:`nti_interfaces.IDynamicSharingTarget`.
        """
        assert IDynamicSharingTarget.providedBy(dynamic_sharing_target)
        wref = IWeakRef(dynamic_sharing_target)
        # pylint: disable=unused-variable
        __traceback_info__ = dynamic_sharing_target, wref
        assert hasattr(wref, 'username')
        # calls jar.readCurrent; returns whether it actually changed
        # pylint: disable=no-member
        if self._dynamic_memberships.add(wref):
            _znotify(StartDynamicMembershipEvent(self, dynamic_sharing_target))

    def record_no_longer_dynamic_member(self, dynamic_sharing_target):
        """
        Records the fact that this object is no longer a member of the given
        dynamic sharing target.

        :param dynamic_sharing_target: The target. Must implement :class:`nti_interfaces.IDynamicSharingTarget`.
        """
        assert IDynamicSharingTarget.providedBy(dynamic_sharing_target)
        _remove_entity_from_named_lazy_set_of_wrefs(
            self, '_dynamic_memberships', dynamic_sharing_target
        )
        _znotify(StopDynamicMembershipEvent(self, dynamic_sharing_target))

    @property
    @deprecate("Prefer `dynamic_memberships` or `usernames_of_dynamic_memberships`")
    def communities(self):
        return self.usernames_of_dynamic_memberships

    @property
    def dynamic_memberships(self):
        """
        An iterable of :class:`nti_interfaces.IDynamicSharingTarget` that we are members of.
        """
        return _iterable_of_entities_from_named_lazy_set_of_wrefs(self, '_dynamic_memberships')

    @property
    def usernames_of_dynamic_memberships(self):
        """ 
        :returns: Iterable names of dynamic sharing targets we belong to. 
        """
        return _set_of_usernames_from_named_lazy_set_of_wrefs(self, '_dynamic_memberships')

    def is_dynamic_member_of(self, entity):
        """
        Test if this object is a dynamic member of the given entity.

        This can be much faster than checking `entity in self.dynamic_memberships`
        """
        # Checking against the weakref avoids waking up all the objects
        try:
            return IWeakRef(entity, None) in self._dynamic_memberships
        except TypeError:
            return False  # "Object has default comparison"

    def _xxx_extra_intids_of_memberships(self):
        """
        Subclasses can extend the set of things that participate in
        security.
        """

    @property
    def xxx_intids_of_memberships_and_self(self):
        """
        A shortcut usable with :meth:`ShareableMixin.xxx_isSharedWithAnyId`.

        :returns: A TreeSet.
        """

        intids = component.getUtility(IIntIds)
        family = intids.family

        result = family.II.TreeSet()
        result.add(intids.getId(self))
        result.update((intids.getId(x) for x in self.dynamic_memberships))
        result.update(self._xxx_extra_intids_of_memberships())
        return result

    def _get_dynamic_sharing_targets_for_read(self):
        return _iterable_of_entities_from_named_lazy_set_of_wrefs(self, '_dynamic_memberships')

    def _get_entities_followed_for_read(self):
        return _iterable_of_entities_from_named_lazy_set_of_wrefs(self, '_entities_followed')

    def _get_stream_cache_containers(self, containerId, context_cache=None):
        if context_cache is None:
            context_cache = _SharingContextCache()
        # start with ours. This ensures things targeted toward us
        # have the highest chance of making it in the cap if we go in order.
        # pylint: disable=no-member
        result = [self.streamCache.getContainer(containerId, ())]

        # add everything we follow. If it's a community, we take the
        # whole thing (ignores are filtered in the parent method),
        # thus getting objects shared only to the community. If it's a
        # person, we take stuff they've shared to communities we're a
        # member of
        # pylint: disable=protected-access
        context_cache._build_entities_followed_for_read(self)
        communities_followed = context_cache.communities_followed

        for following in communities_followed:
            # TODO: Better interface
            result += following._get_stream_cache_containers(containerId)

        # For communities we're simply a member of, but not following,
        # again, pick up stuff shared to the community by people we *are*
        # following.

        # persons_followed = context_cache.persons_followed
        # def community_predicate(change):
        #     try:
        #         return change.creator in persons_followed
        #     except KeyError: #POSKeyError
        #         return False

        # XXX: 20140814: Drop that following restriction for testing...
        # this lets anything that anyone shares with, e.g., 'Everyone' though...
        # XXX: 20140816: A bit too broad on shared environments. Try a similar
        # restriction to what usersearch does and see if we share some
        # non-global community
        persons_followed = context_cache.persons_followed
        # This could be more efficient using the context cache, and could also
        # better handle the global communities...
        remote_com_names = self.usernames_of_dynamic_memberships - {'Everyone'}

        def community_predicate(change):
            try:
                return change.creator in persons_followed \
                    or not remote_com_names.isdisjoint(change.creator.usernames_of_dynamic_memberships)
            except KeyError:  # POSKeyError
                return False
            except AttributeError:
                return False

        result.extend(((comm.streamCache.getContainer(containerId, ()), community_predicate)
                       for comm in context_cache(self._get_dynamic_sharing_targets_for_read)))

        return result

    def getSharedContainer(self, containerId, defaultValue=(), context_cache=None):
        if context_cache is None:
            context_cache = _SharingContextCache()

        # start with ours
        result = LastModifiedCopyingUserList()
        super_result = super(SharingSourceMixin, self).getSharedContainer(
            containerId, defaultValue=defaultValue
        )
        if super_result is not None and super_result is not defaultValue:
            result.extend(super_result)

        # add everything we follow. If it's a community, we take the whole
        # thing (minus ignores). If it's a person, we take stuff they've shared to
        # communities we're a member of (ignores not an issue).
        # Note that to be consistent with the super class interface, we do not
        # de-ref the weak refs in the returned value (even though we must de-ref them
        # internally)
        # TODO: This needs much optimization. And things like paging will
        # be important.
        # pylint: disable=protected-access
        context_cache._build_entities_followed_for_read(self)
        persons_following = context_cache.persons_followed
        communities_seen = context_cache.communities_followed
        for following in communities_seen:
            # pylint: disable=unused-variable
            __traceback_info__ = following
            if following == self:
                continue
            for x in following.getSharedContainer(containerId):
                try:
                    if x is not None and not self.is_ignoring_shared_data_from(x.creator):
                        result.append(x)
                        result.updateLastModIfGreater(x.lastModified)
                except POSKeyError:  # pragma: no cover
                    # an object gone missing. This is bad. NOTE: it may be
                    # something nested in x
                    logger.warning("Shared object (%s) missing in '%s' from '%s' to '%s'",
                                   type(x), containerId, following, self)

        for comm in context_cache(self._get_dynamic_sharing_targets_for_read):
            if comm in communities_seen:
                continue
            for x in comm.getSharedContainer(containerId):
                try:
                    # Communities differ in that we only add things from people we are explicitly
                    # following (unless we are following the entire community; that's handled above)
                    # In some cases (specifically, forums) some objects we create do not get
                    # stored in our containers and only get stored in community shared containers;
                    # we are always implicitly following ourself, so they get added to
                    # XXX This is weird and wrong
                    if x is not None and (x.creator in persons_following or x.creator is self):
                        result.append(x)
                        result.updateLastModIfGreater(x.lastModified)
                except POSKeyError:  # pragma: no cover
                    # an object gone missing. This is bad. NOTE: it may be
                    # something nested in x
                    logger.warning("Shared object (%s) missing in '%s' dynamically shared from '%s' to '%s'",
                                   type(x), containerId, comm, self)

        # If we made no modifications, return the default
        # (which would have already been returned by super; possibly it returned other data)
        if not result:
            return super_result
        return result


from zope.intid.interfaces import IIntIdRemovedEvent


@component.adapter(IDynamicSharingTarget, IIntIdRemovedEvent)
def SharingSourceMixin_dynamicsharingtargetdeleted(target, unused_event):
    """
    Look for things that people could have dynamic memberships recorded
    with, and clear them when deleted.
    """
    # Note: this is on the intid removal, not ObjectRemoved, because
    # by ObjectRemoved time we can't get the intid to match weak refs with

    # This really only matters for IFriendsLists
    # (ICommunity is the only other IDynamicSharingTarget and they don't get deleted)
    # FIXME: No longer true, subclasses of communities can get
    # deleted, in theory, due to courses
    if IFriendsList.providedBy(target):
        for entity in target:
            record_no_longer_dynamic_member = getattr(
                entity, 'record_no_longer_dynamic_member', None
            )
            if callable(record_no_longer_dynamic_member):
                record_no_longer_dynamic_member(target)
                entity.stop_following(target)


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
    def __init__(self, *args, **kwargs):  # pylint: disable=useless-super-delegation
        super(DynamicSharingTargetMixin, self).__init__(*args, **kwargs)


class AbstractReadableSharedMixin(object):
    """
    A mixin for implementing :class:`.IReadableShared`. This class defines everything
    in terms of the ``sharingTargets`` property; subclasses simply need to provide
    an implementation for that property. (For optional efficiency, subclasses can override
    :meth:`_may_have_sharing_targets`)

    """

    def __init__(self):  # pylint: disable=useless-super-delegation
        super(AbstractReadableSharedMixin, self).__init__()

    def _may_have_sharing_targets(self):
        """
        Called in the implementation of the public query methods to aid efficiency. If this
        returns false, then some algorithms may be short-circuited.
        """
        # always assume the worst, so long as the property exists
        return hasattr(self, 'sharingTargets')

    def isSharedDirectlyWith(self, wants):
        """
        Checks if we are directly shared with `wants`, which must be a
        Principal.
        """
        if not self._may_have_sharing_targets():
            return False
        try:
            return wants in self.sharingTargets
        except KeyError:
            pass

    def isSharedIndirectlyWith(self, wants):
        """
        Checks if we are indirectly shared with `wants` (a Principal).
        """

        if not self._may_have_sharing_targets():
            return False

        for target in self.sharingTargets:
            if wants in IEntityContainer(target, ()):
                return True

    def isSharedWith(self, wants):
        """
        Checks if we are directly or indirectly shared with `wants` (a principal)
        """
        return self.isSharedDirectlyWith(wants) or self.isSharedIndirectlyWith(wants)

    @property
    def flattenedSharingTargets(self):
        """
        A flattened :class:`set` of entities with whom this item is shared.
        """
        if not self._may_have_sharing_targets():
            return set()

        result = set()
        for x in self.sharingTargets:
            result.add(x)  # always this one
            # then expand if needed
            iterable = ISharingTargetEntityIterable(x, ())
            result.update(iterable)

        return result

    def getSharingTargetNames(self):
        """
        Returns  a :class:`set` of :class:`SharingTarget` usernames with whom this item
        is shared.
        """
        return {(x.NTIID
                 if IUseNTIIDAsExternalUsername.providedBy(x)
                 else x.username)
                for x in self.sharingTargets}

    def getFlattenedSharingTargetNames(self):
        """
        Returns a flattened :class:`set` of :class:`SharingTarget` usernames with whom this item
        is shared.
        """
        return {(x.NTIID
                 if IUseNTIIDAsExternalUsername.providedBy(x)
                 else x.username)
                for x in self.flattenedSharingTargets}

    # It would be nice to use CachedProperty here, but it doesn't quite play right with
    # object-values for dependent keys
    flattenedSharingTargetNames = property(getFlattenedSharingTargetNames)
    sharingTargetNames = property(getSharingTargetNames)


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

        In general, for internal use, prefer the :attr:`flattenedSharingTargets`
        or :attr:`sharingTargets` property.
        """
        ext_shared_with = []
        for entity in self.sharingTargets:
            # TODO: This entire process does way too much work for as often as this
            # is called so we hack this and couple it tightly to when we think
            # we need to use it. See nti.appserver._adapters
            # ext_shared_with.append( toExternalObject( entity )['Username'] )
            if IUseNTIIDAsExternalUsername.providedBy(entity):
                username = entity.NTIID
            elif (   IUser.providedBy(entity)
                  or ICommunity.providedBy(entity)
                  or IPrincipal.providedBy(entity)):
                username = entity.username
            else:
                # Hmm, what do we have here?
                username = to_external_object(entity)['Username']

            ext_shared_with.append(username)
        return set(ext_shared_with) if ext_shared_with else ()


from nti.dataserver.authentication import _dynamic_memberships_that_participate_in_security

from nti.traversal.traversal import find_interface


class AbstractDefaultPublishableSharedWithMixin(AbstractReadableSharedWithMixin):
    """
    Base class that implements ``sharingTargets`` and ``sharedWith``
    through the presence of the :class:`.IDefaultPublished` interface.
    We say that all instances that are published are shared with all
    the dynamic memberships of the creator or owner of the object (either
    the creator attribute, or the first :class:`.IUser` in our lineage)

    """

    def _may_have_sharing_targets(self):
        return IDefaultPublished.providedBy(self)

    @property
    def sharingTargets(self):
        if IDefaultPublished.providedBy(self):
            return self.sharingTargetsWhenPublished
        return self._non_published_sharing_targets

    _non_published_sharing_targets = ()

    @property
    def sharingTargetsWhenPublished(self):
        creator = getattr(self, 'creator', None)
        # interestingly, IUser does not extend IPrincipal
        owner = creator if IUser.providedBy(creator) else find_interface(self, IUser)
        # TODO: Using a private function
        # This returns a generator, the schema says we need a 'UniqueIterable'
        return _dynamic_memberships_that_participate_in_security(owner, as_principals=False)


def _ii_family():
    intids = component.queryUtility(IIntIds)
    if intids is not None:
        return intids.family
    return BTrees.family64


@interface.implementer(IWritableShared)
class ShareableMixin(AbstractReadableSharedWithMixin, CreatedModDateTrackingObject):
    """ 
    Represents something that can be shared. It has a set of SharingTargets
    with which it is shared (permissions) and some flags. Only its creator
    can alter its sharing targets. It may be possible to copy this object.
    """

    # An IITreeSet of entity intids
    # FIXME: When the user is deleted and his ID goes bad, we're
    # not listening for that. What if the ID gets reused for something else?
    _sharingTargets = None

    def __init__(self):  # pylint: useless-super-delegation
        super(ShareableMixin, self).__init__()

    def _may_have_sharing_targets(self):
        return bool(self._sharingTargets)

    def clearSharingTargets(self):
        if self._sharingTargets is not None:
            self._sharingTargets.clear()  # Preserve existing object
            self.updateLastMod()

    def addSharingTarget(self, target):
        """
        Adds a sharing target. We accept either SharingTarget
        subclasses, or iterables of them.

        """
        if isinstance(target, six.string_types):
            raise TypeError('Strings are no longer acceptable',
                             target, self)

        if      isinstance(target, collections.Iterable) \
            and not isinstance(target, six.string_types) \
            and not isinstance(target, DynamicSharingTargetMixin):
            # TODO: interfaces
            # expand iterables now
            for t in target:
                self.addSharingTarget(t)
            return

        # Don't allow sharing with ourself, it's weird
        # Allow self.creator to be  string or an Entity
        if self.creator == target:
            logger.debug("Dissalow sharing object with creator %s",
                        self.creator)
            return

        if self._sharingTargets is None:
            self._sharingTargets = _ii_family().II.TreeSet()

        self._sharingTargets.add(_getId(target))
        self.updateLastMod()

    def updateSharingTargets(self, replacement_targets, notify=False):
        """
        Cause this object to be shared with only the `replacement_targets` and
        no one else.

        :param replacement_targets: A collection of entities to share with.
        :keyword notify: If True (not the default) then we will broadcast an
                :class:`.IObjectSharingModifiedEvent` when this process is complete.
                Set this to True only outside of externalization (when making a standalone
                call; this is mostly true in tests).
        """
        orig_targets = self.sharingTargets if notify else ()

        replacement_userids = _ii_family().II.TreeSet()

        def addToSet(target):
            if isinstance(target, six.string_types):
                raise TypeError('Strings are no longer acceptable', 
                                target, self)

            if target == self.creator:
                return

            # TODO: interfaces
            if isinstance(target, DynamicSharingTargetMixin):
                replacement_userids.add(_getId(target))
            elif isinstance(target, collections.Iterable):
                for x in target:
                    addToSet(x)
            else:
                replacement_userids.add(_getId(target))

        for target in replacement_targets:
            if target is None:
                continue
            addToSet(target)

        if not replacement_userids:
            self.clearSharingTargets()
            if notify and orig_targets:
                _znotify(
                    ObjectSharingModifiedEvent(self, oldSharingTargets=orig_targets)
                )
            return

        if self._sharingTargets is None:
            self._sharingTargets = replacement_userids
        else:
            self._sharingTargets.update(replacement_userids)

        # Now remove any excess

        # If for some reason we don't actually have sharing targets
        # then this may return None
        excess_targets = _ii_family().II.difference(
            self._sharingTargets, replacement_userids
        )
        for x in (excess_targets or ()):
            self._sharingTargets.remove(x)

        if notify:
            new_targets = self.sharingTargets
            if new_targets != orig_targets:
                _znotify(
                    ObjectSharingModifiedEvent(self, oldSharingTargets=orig_targets)
                )

    def isSharedDirectlyWith(self, wants):
        """
        Checks if we are directly shared with `wants`, which must be a
        Principal.
        """
        if not self._may_have_sharing_targets():
            return False
        # We can be slightly more efficient than the superclass
        try:
            return _getId(wants) in self._sharingTargets
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
        return set((x for x in IntidResolvingIterable(self._sharingTargets,
                                                      allow_missing=True,
                                                      parent=self,
                                                      name='sharingTargets')
                    if x is not None and hasattr(x, 'username')))

    def xxx_isReadableByAnyIdOfUser(self, remote_user, ids, family=None):
        """
        Shortcut convenience method to check if this object is shared with
        any of the intids in ids. These ids come from the
        flattened membership ids.
        """
        # pylint: disable=unused-variable
        __traceback_info__ = remote_user, ids, family
        if not self._may_have_sharing_targets():
            return False
        family = _ii_family() if family is None else family
        return bool(family.II.intersection(self._sharingTargets, ids))
