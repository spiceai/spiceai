def validate_rewards(action_rewards: "dict[str]") -> bool:
    for action_name in action_rewards:
        if (
            "reward =" not in action_rewards[action_name]
            and "reward=" not in action_rewards[action_name]
        ):
            return False
    return True
