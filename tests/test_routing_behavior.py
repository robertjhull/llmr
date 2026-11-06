from __future__ import annotations

import unittest

# guidelines for routing behavior tests
# - simulate the llm calls to control the timings
# - timings can be shortened/compressed to avoid overly long tests

class RoutingBehaviorTests(unittest.TestCase):
    def setUp(self):
        # if needed
        return

    def test_ten_queued_against_two_models_same_speed_equal_distribution(self):
        # TODO
        # 10 requests should be simulated against a pool with two models
        # the two models should have equal tok/s
        # each model should process 5 requests
        return

    def test_ten_queued_against_two_models_different_speed_unequal_distribution(self):
        # TODO
        # 10 requests should be simulated against a pool with two models
        # one model should work at double the tok/s of the other
        # the faster model should consume at least double the requests as the other
        return

    def test_ten_queued_against_two_models_changing_speed_changes_distribution(self):
        # TODO
        # 10 requests should be simulated against a pool with two models
        # one model should initially work at double the tok/s of the other
        # the faster model should process 2 requests for the other models 1
        # after processing the first 2 requests, the faster model's speed degrades to half the speed of the other
        # the other model should now process 2 requests for every 1 that the first model does
        return

    def test_ten_queued_against_two_models_model_errors_out_routes_remaining_requests_to_healthy_model(self):
        # TODO
        # 10 requests should be simulated against a pool with two models
        # one of the models errors out enough to trigger its cooldown/removal from queue availability
        # all remaining requests are processed by the healthy model
        return

    def test_ten_queued_against_two_models_both_models_error_out(self):
        # TODO
        # 10 requests should be simulated against a pool with two models
        # both models error out
        # all remaining requests return a valid failed response
        return

    def test_one_request_fails_on_model_retries_on_other_models(self):
        # TODO
        # 1 request sent to pool with 3 models with retry policy of 2x
        # model a fails, route #1 retry to model b
        # model b fails, route #2 retry to model c
        # model c fails, request returns as failed response
        return

    def test_model_in_cooldown_becomes_eligible_again_after_passing_health_check(self):
        # TODO
        # a model errors out enough to trigger cooldown/removal from queue availability
        # the model subsequently passes a health check (or its cooldown window elapses)
        # new requests are routed to it again once it is back in rotation
        return

    def test_cooldown_requires_a_consecutive_failure_threshold_not_a_single_blip(self):
        # TODO
        # a model fails one request but then succeeds on the next
        # one failure alone should not trigger cooldown/removal from queue availability
        # the model should still receive new requests after the single failure
        return

    def test_failure_streak_resets_after_an_intervening_successful_call(self):
        # TODO
        # a model fails N-1 requests (one short of the cooldown threshold), then succeeds once
        # a further N-1 failures afterward should still not trigger cooldown
        # i.e. the failure count resets on a success rather than accumulating indefinitely
        return

    def test_retry_succeeds_on_second_model_after_first_model_fails(self):
        # TODO
        # 1 request sent to a pool with 2 models
        # model a fails, request retries on model b
        # model b succeeds
        # the result reports model b's id and the correct attempt count
        return

    def test_retry_does_not_reuse_a_model_already_failed_for_this_job(self):
        # TODO
        # 1 request sent to a pool with 3 models with retry policy of 2x
        # model a fails, retry lands on model b (not model a again)
        # model b fails, retry lands on model c (not model a or b again)
        return

    def test_retry_attempts_are_capped_at_the_configured_max_retries(self):
        # TODO
        # 1 request sent to a pool with more healthy models than max_retries allows
        # every attempt fails
        # exactly max_retries attempts are made, no more and no fewer, before returning failed
        return

    def test_oversized_prompt_only_routes_to_deployments_with_sufficient_context_window(self):
        # TODO
        # a pool has one deployment with a small max_context_tokens and one with a large/no limit
        # a prompt that exceeds the small deployment's limit is only ever routed to the larger one
        # a prompt that exceeds every deployment's limit fails immediately instead of queuing forever
        return

    def test_pinned_deployment_is_used_even_when_a_faster_deployment_is_idle(self):
        # TODO
        # a request is pinned to a specific deployment id in a pool with a faster, idle alternative
        # the request is still routed to the pinned deployment
        # if the pinned deployment is busy or unhealthy, the request waits/fails rather than rerouting
        return

    def test_busy_deployment_is_preferred_only_when_it_clears_the_margin_threshold(self):
        # TODO
        # a busy deployment's projected finish time (remaining + new job) is compared against an idle deployment's estimate
        # when the busy deployment's projected total is faster by at least the preference margin, a new job waits for it
        # when it falls just short of that margin, the new job takes the open idle slot instead
        return