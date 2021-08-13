from algorithms.agent_interface import SpiceAIAgent
from algorithms.dql.agent import DeepQLearning_Agent
from algorithms.vpg.agent import VanillaPolicyGradient_Agent


def get_agent(name: str, state_shape, action_size: int) -> SpiceAIAgent:
    if name == "vpg":
        return VanillaPolicyGradient_Agent(state_shape, action_size)
    elif name == "dql":
        return DeepQLearning_Agent(state_shape, action_size)

    raise NotImplementedError(f"Unable to find agent with name '{name}'")
