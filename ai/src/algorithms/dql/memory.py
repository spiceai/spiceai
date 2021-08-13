from collections import deque
import random
import numpy as np


class ReplayBuffer:
    def __init__(self, batch_size):
        self.buffer = deque()
        self.batch_size = batch_size

    def store(self, state, action, reward, next_state):
        self.buffer.append([state, action, reward, next_state])

    def sample(self):
        sample = random.sample(self.buffer, self.batch_size)
        states, actions, rewards, next_states = map(np.asarray, zip(*sample))
        states = np.array(states).reshape(self.batch_size, -1)
        next_states = np.array(next_states).reshape(self.batch_size, -1)
        return states, actions, rewards, next_states

    def size(self):
        return len(self.buffer)
