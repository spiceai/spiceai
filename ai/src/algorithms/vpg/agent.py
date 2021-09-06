from algorithms.agent_interface import SpiceAIAgent
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, models
from tensorflow.keras import backend as K
from algorithms.vpg.memory import Memory
from exception import InvalidDataShapeException
import warnings
import os


def build_networks(state_shape, action_size, learning_rate, hidden_neurons):
    """Creates a Policy Gradient Neural Network.

    Creates a two hidden-layer Policy Gradient Neural Network. The loss
    function is altered to be a log-likelihood function weighted
    by the discounted reward, g.

    Args:
        space_shape: a tuple of ints representing the observation space.
        action_size (int): the number of possible actions.
        learning_rate (float): the nueral network's learning rate.
        hidden_neurons (int): the number of neurons to use per hidden
            layer.
    """
    state_input = layers.Input(state_shape, name="state")
    g = layers.Input((1,), name="g")

    hidden_1 = layers.Dense(hidden_neurons, activation="relu")(state_input)
    hidden_2 = layers.Dense(hidden_neurons, activation="relu")(hidden_1)
    probabilities = layers.Dense(action_size, activation="softmax")(hidden_2)

    def custom_loss(y_true, y_pred):
        CLIP_EDGE = 1e-8
        y_pred_clipped = K.clip(y_pred, CLIP_EDGE, 1 - CLIP_EDGE)
        log_lik = y_true * K.log(y_pred_clipped)
        return K.sum(-log_lik * g)

    policy = models.Model(inputs=[state_input, g], outputs=[probabilities])
    optimizer = tf.keras.optimizers.Adam(learning_rate=learning_rate)
    policy.compile(loss=custom_loss, optimizer=optimizer)

    predict = models.Model(inputs=[state_input], outputs=[probabilities])

    # Useful for visualizing the neural network graph
    # tf.keras.utils.plot_model(predict, "predict_model.png", show_shapes=True)
    return policy, predict


class VanillaPolicyGradient_Agent(SpiceAIAgent):
    """Sets up a reinforcement learning agent."""

    def __init__(
        self, state_shape, action_size, gamma=0.9, learning_rate=0.02, hidden_neurons=10
    ):
        """Initializes the agent with Policy Gradient networks
            and memory sub-classes.

        Args:
            state_shape: The shape of the observation state
            action_size: How many actions our agent is able to take.
            gamma: The discount factor for rewards that occur earlier on.
        """
        super().__init__(state_shape, action_size)

        policy, predict = build_networks(
            state_shape, action_size, learning_rate, hidden_neurons
        )
        self.policy = policy
        self.predict = predict
        self.action_size = action_size
        self.gamma = gamma
        self.memory = Memory()

        warnings.simplefilter(action="ignore", category=Warning)

    def add_experience(self, state, action, reward, _):
        self.memory.add((state, action, reward))

    def act(self, state):
        """Selects an action for the agent to take given a game state.

        Args:
            state (list of numbers): The state of the environment to act on.

        Returns:
            (int) The index of the action to take.
        """
        # If not acting randomly, take action with highest predicted value.
        state_batch = np.expand_dims(state, axis=0)
        try:
            probabilities = self.predict.predict(state_batch, verbose=0)[0]
        except ValueError as ex:
            if "expected state to have shape" in str(ex):
                raise InvalidDataShapeException(str(ex))
            raise ex

        # print(probabilities)
        action = np.random.choice(len(probabilities), p=probabilities)
        return action, probabilities

    def discount_episode(self, rewards, gamma):
        discounted_rewards = np.zeros_like(rewards)
        total_rewards = 0
        for t in reversed(range(len(rewards))):
            total_rewards = rewards[t] + total_rewards * gamma
            discounted_rewards[t] = total_rewards
        return discounted_rewards

    def learn(self):
        """Trains a Policy Gradient policy network based on stored experiences."""
        state_mb, action_mb, reward_mb = self.memory.sample()
        # One hot encode actions
        actions = np.zeros([len(action_mb), self.action_size])
        actions[np.arange(len(action_mb)), action_mb] = 1

        # Apply TD(1)
        discount_mb = self.discount_episode(reward_mb, self.gamma)
        std_dev = 1 if np.std(discount_mb) == 0 else np.std(discount_mb)

        discount_mb = (discount_mb - np.mean(discount_mb)) / std_dev
        return self.policy.train_on_batch([state_mb, discount_mb], actions)

    def save(self, model_name):
        self.predict.save(model_name)

    def load(self, model_name):
        if os.path.exists(model_name):
            self.predict = models.load_model(model_name)
            return True
        else:
            print(f"Model {model_name} doesn't exist")
            return False
