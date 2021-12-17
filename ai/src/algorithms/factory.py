from pathlib import Path
from algorithms.agent_interface import SpiceAIAgent
from algorithms.dql.agent import DeepQLearningAgent
from algorithms.vpg.agent import VanillaPolicyGradientAgent
from algorithms.sacd.agent import SoftActorCriticDiscreteAgent


def get_agent(
    name: str, state_shape, action_size: int, loggers, log_dir: Path
) -> SpiceAIAgent:
    if name == "vpg":
        return VanillaPolicyGradientAgent(state_shape, action_size, loggers, log_dir)

    if name == "dql":
        return DeepQLearningAgent(state_shape, action_size, loggers, log_dir)

    if name == "sacd":
        return SoftActorCriticDiscreteAgent(state_shape, action_size, loggers, log_dir)

    raise NotImplementedError(
        f"Unable to find agent for the learning algorithm '{name}'"
    )
