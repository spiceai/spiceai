import abc

"""
The interface that all Spice AI compatible deep-RL agents should comform to.
"""


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
    def act(self, state) -> int:
        """
        Returns an action recommended by the algorithm's policy for the current state

        Args:
            state: The observation state to act upon

        Returns:
            (int) The action to take, as an integer that can index into the array of defined actions.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def add_experience(self, state, action, reward):
        """
        Adds the experience of the reward from taking this action at that state

        Args:
            state: The observation state when the action was taken
            action: The action that was performed
            reward: The reward that was received for taking that action
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def learn(self):
        """
        Updates the algorithm's policy based on its collected experiences so far.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def save(self, model_path: str):
        """
        Save the trained model to disk

        Args:
            model_path: The path on disk to save the model parameters to
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def load(self, model_path: str) -> bool:
        """
        Load a previously trained model from disk. Returns whether it was able to load the model.

        Args:
            model_path: The path on disk to load the model parameters from

        Returns:
            (bool) Whether the model was able to be loaded properly
        """
        raise NotImplementedError()
