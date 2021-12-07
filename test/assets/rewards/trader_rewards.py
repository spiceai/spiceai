def calculate_price_change(curr_state: dict, next_state: dict):
    curr_price = curr_state["coinbase_btcusd_close"]
    next_price = next_state["coinbase_btcusd_close"]
    return curr_price - next_price


def buy(curr_state: dict, prev_interps, next_state: dict, new_interps):
    return calculate_price_change(curr_state=curr_state, next_state=next_state)


def sell(curr_state: dict, prev_interps, next_state: dict, new_interps):
    return -calculate_price_change(curr_state=curr_state, next_state=next_state)


def hold(curr_state: dict, prev_interps, next_state: dict, new_interps):
    return -0.1
