"""
The interface that all Spice.ai compatible deep-RL agents should conform to.
"""

import abc
from pathlib import Path
from typing import Tuple


class SpiceAIAgent(metaclass=abc.ABCMeta):
    """
    Args:
        state_shape: The shape of the observation state
        action_size: How many actions our agent is able to take.
    """

    def __init__(self, state_shape, action_size):
        self.state_shape = state_shape
        self.action_size = action_size

    @abc.abstractmethod
    def act(self, state) -> Tuple[int, list]:
        """
        Returns an action recommended by the algorithm's policy for the current state

        Args:
            state: The observation state to act upon

        Returns:
            (int): The action to take, as an integer that can index into the array of defined actions.
            (list): The probabilities of each action.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def add_experience(self, state, action, reward, next_state):
        """
        Adds the experience of the reward from taking this action at that state

        Args:
            state: The observation state when the action was taken
            action: The action that was performed
            reward: The reward that was received for taking that action
            next_state: The observation state after the action was taken
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def learn(self):
        """
        Updates the algorithm's policy based on its collected experiences so far.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def save(self, path: Path):
        """
        Save the trained model to disk

        Args:
            model_path: The path on disk to save the model parameters to
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def load(self, path: Path) -> bool:
        """
        Load a previously trained model from disk. Returns whether it was able to load the model.

        Args:
            model_path: The path on disk to load the model parameters from

        Returns:
            (bool) Whether the model was able to be loaded properly
        """
        raise NotImplementedError()
