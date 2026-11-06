from __future__ import annotations

import unittest

# guidelines for status behavior tests
# - simulate the llm calls to control the timings

class StatusBehaviorTests(unittest.TestCase):
    def setUp(self):
        # if needed
        return

    def test_pools_show_accurate_queued_status(self):
        # TODO
        # send 4 requests to one pool with two healthy models
        # status page shows both models busy working on their respective requests with the pool showing 2 as queued
        # as each model finishes and accepts a new job the queue count decrements
        return

    def test_pools_show_all_statuses(self):
        # TODO
        # send 1 request to one pool with one model
        # the request should flow through all possible stages a single job might pass through
        # for example, test a request that requires both retry and tool-calling
        # status might progress as follows:
        # inferring -> retrying (failed Nx) -> inferring -> streaming -> requested tool -> inferring -> streaming -> idle
        # all possible states should be tested
        return

    def test_failing_model_shows_cooldown(self):
        # TODO
        # send 1 request to one pool with one model
        # the request fails enough to trigger a cooldown
        # the model shows on the status page as being in cooldown/unhealthy
        return

    def test_pools_do_not_leak_queued_or_active_status_into_each_other(self):
        # TODO
        # two separate pools, each with its own models and in-flight requests
        # queued count and active-call status for pool A must reflect only pool A's requests
        # activity in pool B must not affect pool A's reported queue depth or deployment statuses
        return