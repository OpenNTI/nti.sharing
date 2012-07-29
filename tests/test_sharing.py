#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
$Id$
"""
from __future__ import print_function, unicode_literals

from hamcrest import assert_that
from hamcrest import has_length
from hamcrest import is_not, is_
does_not = is_not
from hamcrest import has_item
from nti.dataserver.tests.mock_dataserver import ConfiguringTestBase

import fudge

import persistent

from nti.dataserver.sharing import _SharedStreamCache as StreamCache


class Change(persistent.Persistent):
	id = None
	lastModified = None
	containerId = 'foo'
	@property
	def object(self): return self

	def __repr__( self ):
		return "<Change %s %s>" % (self.id, self.lastModified)

class TestStreamSharedCache(ConfiguringTestBase):

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
