from __future__ import annotations

import unittest

# guidelines for job lifecycle behavior tests
# - simulate the llm calls to control the timings
# - timings can be shortened/compressed to avoid overly long tests

class JobLifecycleBehaviorTests(unittest.TestCase):
    def setUp(self):
        # if needed
        return

    def test_router_loads_persisted_metrics_on_startup_and_uses_them_immediately(self):
        # TODO
        # requests are processed and metrics are persisted to disk
        # a new router instance is created pointed at the same metrics path
        # its first scheduling decision uses the persisted tok/s values rather than starting cold
        return

    def test_cancelling_a_queued_job_releases_it_without_ever_claiming_a_slot(self):
        # TODO
        # a job is submitted to a pool that is fully saturated, so it waits for a slot
        # the job is cancelled while still queued, before it ever claims a deployment
        # the job's status becomes cancelled and queue depth decrements accordingly
        return

    def test_cancelling_a_job_mid_inference_releases_its_claimed_slot_for_others(self):
        # TODO
        # a job claims a deployment slot and is actively inferring
        # the job is cancelled mid-flight
        # the job's status becomes cancelled and the deployment's slot is freed for the next queued job
        return

    def test_freed_slot_is_claimed_by_exactly_one_waiting_job(self):
        # TODO
        # several jobs are queued waiting on a single-capacity deployment
        # when the in-flight job completes and frees its slot, exactly one waiting job claims it
        # no two jobs end up claiming the same freed slot
        return

    def test_tool_round_limit_exceeded_without_final_response_format_fails_the_job(self):
        # TODO
        # a job with tools configured and no final_response_format hits its max_tool_rounds limit
        # with an unresolved pending tool call
        # the job fails with a reason indicating the tool round limit was exceeded
        return
