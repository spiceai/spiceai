from typing import Dict


def validate_rewards(action_rewards: Dict[str, str], external_reward_funcs: str) -> bool:
    if len(external_reward_funcs) > 0:
        return True

    for action_name in action_rewards:
        if "reward =" not in action_rewards[action_name] and "reward=" not in action_rewards[action_name]:
            return False
    return True
