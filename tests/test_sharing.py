#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import

#disable: accessing protected members, too many methods
#pylint: disable=W0212,R0904

import unittest
from hamcrest import assert_that
from hamcrest import has_length
from hamcrest import has_property
from hamcrest import is_not, is_
does_not = is_not
from hamcrest import not_none
from hamcrest import has_item
from nti.dataserver.tests.mock_dataserver import SharedConfiguringTestLayer
from nti.testing.matchers import is_false
from nti.testing.time import time_monotonically_increases

import fudge
import time
import persistent


from nti.dataserver.sharing import _SharedStreamCache as StreamCache
from nti.dataserver.sharing import SharingTargetMixin
from nti.dataserver.sharing import _SharingContextCache

class Change(persistent.Persistent):
	id = None
	lastModified = None
	containerId = 'foo'
	creator = None
	@property
	def object(self): return self

	def __repr__( self ):
		return "<Change %s %s %s>" % (self.id, self.lastModified, self.containerId)

class TestStreamSharedCache(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	@fudge.patch( 'nti.dataserver.sharing._getId' )
	def test_size_caps(self, fake_getId):
		def getId( obj, default=None ):
			return obj.id
		fake_getId.is_callable().calls( getId )

		cache = StreamCache()
		cache.stream_cache_size = 3

		c1 = Change(); c1.id = 1; c1.lastModified = 1
		c2 = Change(); c2.id = 2; c2.lastModified = 2
		c3 = Change(); c3.id = 3; c3.lastModified = 3
		c32 = Change(); c32.id = 3; c32.lastModified = 4
		c4 = Change(); c4.id = 4; c4.lastModified = 5

		cache.addContainedObject( c1 )
		cache.addContainedObject( c2 )
		cache.addContainedObject( c3 )
		assert_that( list(cache.getContainer( c1.containerId )), has_length( 3 ) )

		# OK. At max.
		# Adding a newer change for the same object will replace the existing
		# change, but the rest will remain
		def keys():
			return list( cache._containers[c1.containerId].keys() )
		def values():
			return list( cache._containers[c1.containerId].values() )
		cache.addContainedObject( c32 )

		assert_that( list(cache.getContainer( c1.containerId )), has_length( 3 ) )
		for i in (c1, c2, c32):
			assert_that( values(), has_item( i ) )
			assert_that( keys(), has_item( i.id ) )

		# An even newer change will bump the oldest object
		cache.addContainedObject( c4 )
		assert_that( list(cache.getContainer( c1.containerId )), has_length( 3 ) )
		for i in (c2, c32, c4):
			assert_that( values(), has_item( i ) )
			assert_that( keys(), has_item( i.id ) )

		c5 = Change(); c5.id = 5; c5.lastModified = 6
		# If we delete an item, we can add a new one
		cache.deleteEqualContainedObject( c4 )
		assert_that( list(cache.getContainer( c1.containerId )), has_length( 2 ) )
		cache.addContainedObject( c5 )
		assert_that( list(cache.getContainer( c1.containerId )), has_length( 3 ) )
		for i in (c2, c32, c5):
			assert_that( values(), has_item( i ) )
			assert_that( keys(), has_item( i.id ) )


	def test_muted_none_container_id( self ):
		class SharingTarget(SharingTargetMixin,persistent.Persistent): pass
		sharingtarget = SharingTarget()
		assert_that( sharingtarget._muted_oids, is_( not_none() ) )
		class ObjWithNoneContainerId(object):
			containerId = None

		assert_that( sharingtarget.is_muted( ObjWithNoneContainerId() ), is_false() )

	@time_monotonically_increases
	@fudge.patch( 'nti.dataserver.sharing._getId' )
	def test_stream_paging(self, fake_getId):
		def getId( obj, default=None ):
			return obj.id
		fake_getId.is_callable().calls( getId )

		# Between two containers, lets split up a large number of changes

		class SharingTarget(SharingTargetMixin,persistent.Persistent):
			pass
		sharingtarget = SharingTarget()
		sharingtarget.MAX_STREAM_SIZE = 500 # cache way more than we need
		sharingtarget.username = 'foo@bar'

		earliest_id = time.time() + 1
		all_changes = [] # from oldest to newest
		for _ in range(200): # So 400 total objects
			for cid in 'c1', 'c2':
				change = Change()
				change.id = change.lastModified = time.time()
				change.containerId = cid
				change.creator = 'me'
				all_changes.append( change )
				sharingtarget._addToStream( change )
		latest_id = time.time() - 1

		# First, make sure we can get them all
		# from containers individually, sorted descending
		result = sharingtarget.getContainedStream( 'c1', maxCount=500 )
		assert_that( result, has_length( 200 ) )
		assert_that( result[-1], has_property( 'id', earliest_id ) )
		result = sharingtarget.getContainedStream( 'c2', maxCount=500 )
		assert_that( result, has_length( 200 ) )
		assert_that( result[0], has_property( 'id', latest_id ) )

		# We can get them all if we accumulate
		context_cache = _SharingContextCache()
		context_cache.make_accumulator()
		sharingtarget.getContainedStream( 'c1', maxCount=500, context_cache=context_cache )
		sharingtarget.getContainedStream( 'c2', maxCount=500, context_cache=context_cache )
		result = context_cache.to_result()
		assert_that( result, has_length( 400 ) )
		assert_that( result[0], has_property( 'id', latest_id ) )
		assert_that( result[-1], has_property( 'id', earliest_id ) )

		# We can get one page, split across the two containers
		context_cache = _SharingContextCache()
		context_cache.make_accumulator()
		sharingtarget.getContainedStream( 'c1', maxCount=100, context_cache=context_cache )
		sharingtarget.getContainedStream( 'c2', maxCount=100, context_cache=context_cache )
		result = context_cache.to_result()

		assert_that( result, has_length( 100 ) )
		# The most recent 100 changes
		assert_that( result, is_( list(reversed(all_changes[300:])) ) )

		# Now we can take that and get the most recent 10 changes before them
		before = result[-1].lastModified
		context_cache = _SharingContextCache()
		context_cache.make_accumulator()
		sharingtarget.getContainedStream( 'c1', maxCount=10, before=before, context_cache=context_cache )
		sharingtarget.getContainedStream( 'c2', maxCount=10, before=before, context_cache=context_cache )
		result = context_cache.to_result()

		assert_that( result, has_length( 10 ) )
		# Newest in this set is exactly one before we started
		assert_that( result[0], has_property( 'id', before - 1 ) )
		expected = list( reversed( all_changes[290:300] ) )
		assert_that( result, is_( expected ) )
