from azure import durable_functions as df
from datetime import datetime
import logging
from src.log_utils import setup_logger
from dataclasses import dataclass, asdict, fields
from typing import Self

logger = setup_logger(__name__, logging.INFO)

@dataclass
class RateLimiterState:
    remaining: int          # allowable requests in current window
    used_previous: int      # already allowed in previous window
    used_current: int       # already allowed in current window
    last_success_time: str       # epoch minute of most recent allowed request 

    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        arg_names = [f.name for f in fields(cls)]
        return cls(**{k:v for k,v in data.items() if k in arg_names})

    @classmethod
    def default(cls, rpm: int, success_time: datetime) -> Self:
        # Remaining obv defaults to the max rate.
        # Last success time can wlog default to now, because
        # theoretically we'd success from a clean slate.
        params = dict(
            remaining = rpm,
            used_previous = 0,
            used_current = 0,
            last_success_time = success_time.isoformat()
        )
        logger.debug(f"Creating default state {params}")
        return RateLimiterState(**params)
     

GET_STATUS = "GET_STATUS"
TRY_ACQUIRE = "TRY_ACQUIRE"

def rate_limiter_entity(context: df.DurableEntityContext):
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    input_data = context.get_input()
    logger.debug(f"Entering rate limiter: {input_data}")

    if input_data is None:
        raise ValueError("Rate limiter missing input data.")
    
    operation = context.operation_name
    if operation not in [GET_STATUS, TRY_ACQUIRE]:
        raise ValueError(f"Invalid operation name {operation}")

    rate_limit_rpm = input_data.get("rate_limit_rpm", 10)
    current_time = datetime.now()
    state = context.get_state(lambda: RateLimiterState.default(rate_limit_rpm, current_time).to_dict())
    state = RateLimiterState.from_dict(state)

    if operation == GET_STATUS:
        # This operation is mostly for testing purposes
        # Make sure to set an informative state.
        context.set_state(state.to_dict()) # Return meaningful state
        context.set_result(True)           # Return non-meaningful result
        return
    
    # Compute current window
    # We only care about two times: actually right now and the last success time
    # We don't care when the task was originally submitted. What matters is we're seeing it now.
    current_window = current_time.timestamp() // 60
    last_success = datetime.fromisoformat(state.last_success_time).timestamp() // 60

    if last_success == current_window:
        # don't need to change used counts
        pass 
    elif last_success == current_window - 1:
        # shift current count into previous
        state.used_previous = state.used_current
        state.used_current = 0
    else:
        # a full window has elapsed. reset both counts
        state.used_current = 0
        state.used_previous = 0
    
    overlap = current_time.second
    overlap_weight = (60 - overlap) / 60
    used_total = state.used_previous * overlap_weight + state.used_current

    if used_total < rate_limit_rpm:
        state.used_current += 1
        state.remaining = rate_limit_rpm - state.used_current - state.used_previous
        state.last_success_time = current_time.isoformat()
        context.set_result(True)
    else:
        context.set_result(False)
    
    logger.debug(f"Rate limiter exited with state {state}")
    context.set_state(state.to_dict())
