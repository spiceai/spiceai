import numpy as np


class Memory:
    """Sets up a memory replay buffer for Policy Gradient methods."""

    def __init__(self):
        self.buffer = []

    def add(self, experience):
        """Adds an experience into the memory buffer.

        Args:
            experience: a (state, action, reward) tuple.
        """
        self.buffer.append(experience)

    def clear(self):
        self.buffer = []

    def sample(self):
        """Returns the list of episode experiences and clears the buffer.

        Returns:
            (list): A tuple of lists with structure (
                [states], [actions], [rewards]
            }
        """
        batch = np.array(self.buffer, dtype=object).T.tolist()
        states_mb = np.array(batch[0], dtype=np.float32)
        actions_mb = np.array(batch[1], dtype=np.int8)
        rewards_mb = np.array(batch[2], dtype=np.float32)
        self.buffer = []
        return states_mb, actions_mb, rewards_mb
