from azure import durable_functions as df
from datetime import datetime, timezone, timedelta
import logging
from src.log_utils import setup_logger
from functools import partial
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Literal, Self

logger = setup_logger(__name__, logging.DEBUG)

@dataclass
class RateLimiterState:
    remaining: int          # allowable requests in current window
    used_previous: int      # already allowed in previous window
    used_current: int       # already allowed in current window
    current_time: str       # epoch minute of most recent allowed request 

    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return RateLimiterState(**data)

    @classmethod
    def default(cls, rpm: int, current_time: datetime) -> Self:
        params = dict(
            remaining = rpm,
            used_previous = 0,
            used_current = 0,
            current_time = current_time.isoformat()
        )        
        return RateLimiterState(**params)
     

GET_STATUS = "GET_STATUS"
TRY_ACQUIRE = "TRY_ACQUIRE"
Operations = Literal[GET_STATUS, TRY_ACQUIRE]

def rate_limiter_entity(context: df.DurableEntityContext):
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    logger.debug(f"Calling rate limiter with entity_key: {context.entity_key}")
    
    input_data = context.get_input()

    if input_data is None:
        raise ValueError("Rate limiter missing input data.")
    
    operation = context.operation_name
    if operation not in [GET_STATUS, TRY_ACQUIRE]:
        raise ValueError(f"Invalid operation name {operation}")

    current_time_str = input_data.get("current_time", datetime.now(timezone.utc).isoformat())
    current_time = datetime.fromisoformat(current_time_str)
    rate_limit_rpm = input_data.get("rate_limit_rpm")
    state = context.get_state(lambda: RateLimiterState.default(rate_limit_rpm, current_time).to_dict())
    state = RateLimiterState.from_dict(state)
    assert isinstance(state, RateLimiterState) # For type hints

    
    if operation == GET_STATUS:
        # This operation is mostly for testing purposes
        # Make sure to set an informative state.
        context.set_state(state.to_dict()) # Return meaningful state
        context.set_result(True)           # Return non-meaningful result
        return
    
    # Compute current window
    current_window = current_time.timestamp() // 60
    state_window = datetime.fromisoformat(state.current_time).timestamp() // 60

    if state_window == current_window:
        # don't need to change used counts
        pass 
    elif state_window == current_window - 1:
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
        state.current_time = current_time_str
        context.set_result(True)
    else:
        context.set_result(False)
    
    context.set_state(state.to_dict())
    logger.debug(f"Rate limiter finished with state: {context.get_state()}")
