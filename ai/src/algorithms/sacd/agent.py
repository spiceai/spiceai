import json
from pathlib import Path
from typing import Tuple

import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import optimizers
from tensorflow.keras import layers
import tensorflow_probability as tfp

from algorithms.agent_interface import SpiceAIAgent
from algorithms.dql.memory import ReplayBuffer

tf.keras.backend.set_floatx("float64")


class SACD(keras.Model):
    ACTIVATION = 'leaky_relu'
    LEARNING_RATE = 1e-3
    REWARD_DISCOUNT = 0.95
    TARGET_ENTROPY_SCALE = 0.2
    TARGET_MOMEMTUM = 0.05

    @staticmethod
    def create_network(input_dim: int, output_dim: int, final_activation: str = None) -> keras.Model:
        return keras.Sequential([
            keras.Input(input_dim),
            layers.Dense(128, activation=SACD.ACTIVATION),
            layers.Dense(128, activation=SACD.ACTIVATION),
            layers.Dense(64, activation=SACD.ACTIVATION),
            layers.Dense(32, activation=SACD.ACTIVATION),
            layers.Dense(output_dim, activation=final_activation)])

    class Actor(keras.Model):

        def __init__(self, state_dim: int, action_dim: int):
            super().__init__()
            self.seq = SACD.create_network(state_dim, action_dim, 'softmax')

        def call(self, input_tensor: tf.Tensor) -> Tuple[tf.Tensor, tf.Tensor]:
            action_probs = self.seq(input_tensor)
            distribution = tfp.distributions.Categorical(action_probs)
            return distribution.sample(), action_probs

    def __init__(self, state_shape: tuple, action_size):
        super().__init__()
        self.state_shape = state_shape
        self.action_size = action_size

        self.actor = self.Actor(state_shape[0], action_size)
        self._critic_1 = SACD.create_network(state_shape[0], action_size)
        self._critic_2 = SACD.create_network(state_shape[0], action_size)
        self._target_critic_1 = SACD.create_network(state_shape[0], action_size)
        self._target_critic_2 = SACD.create_network(state_shape[0], action_size)

        self.target_entropy = -np.log((1.0 / action_size)) * self.TARGET_ENTROPY_SCALE
        self.log_alpha = tf.Variable([1.0], trainable=True, name='log_alpha', dtype=tf.float64)
        self.alpha = tf.exp(self.log_alpha)

        self._actor_variables = self.actor.trainable_variables
        self._critic_1_variables = self._critic_1.trainable_variables
        self._critic_2_variables = self._critic_2.trainable_variables
        self._target_critic_1_variables = self._target_critic_1.trainable_variables
        self._target_critic_2_variables = self._target_critic_2.trainable_variables

        for critic_var, target_var in zip(self._critic_1_variables, self._target_critic_1_variables):
            target_var.assign(critic_var)
        for critic_var, target_var in zip(self._critic_2_variables, self._target_critic_2_variables):
            target_var.assign(critic_var)

        self._actor_optimizer = optimizers.Adam(learning_rate=self.LEARNING_RATE)
        self._critic_1_optimizer = optimizers.Adam(learning_rate=self.LEARNING_RATE)
        self._critic_2_optimizer = optimizers.Adam(learning_rate=self.LEARNING_RATE)
        self._alpha_optimizer = optimizers.Adam(learning_rate=self.LEARNING_RATE)

    def call(self, input_tensor: tf.Tensor) -> Tuple[tf.Tensor, tf.Tensor]:
        return self.actor(input_tensor)

    def _copy_target_models(self):
        for critic_var, target_var in zip(self._critic_1_variables, self._target_critic_1_variables):
            target_var.assign(self.TARGET_MOMEMTUM * critic_var + (1.0 - self.TARGET_MOMEMTUM) * target_var)
        for critic_var, target_var in zip(self._critic_2_variables, self._target_critic_2_variables):
            target_var.assign(self.TARGET_MOMEMTUM * critic_var + (1.0 - self.TARGET_MOMEMTUM) * target_var)

    def train(self, data):
        state_batch, action_batch, reward_batch, next_state_batch = data
        action_batch = tf.cast(tf.expand_dims(action_batch, 1), tf.float64)
        reward_batch = tf.cast(tf.expand_dims(reward_batch, 1), tf.float64)

        with tf.name_scope('actor_loss'):
            with tf.GradientTape() as actor_tape:
                _action, action_probs = self.actor(state_batch)
                action_logprobs = tf.math.log(action_probs)
                q1_value = self._critic_1(state_batch)
                q2_value = self._critic_2(state_batch)
                q_log_target = tf.minimum(q1_value, q2_value)
                actor_loss = tf.reduce_mean(
                    tf.reduce_sum(action_probs * (self.alpha * action_logprobs - q_log_target), 1))
        self._actor_optimizer.apply_gradients(
            zip(actor_tape.gradient(actor_loss, self._actor_variables), self._actor_variables))

        with tf.name_scope('critic_loss'):
            _next_action, next_action_probs = self.actor(next_state_batch)
            next_action_logprobs = tf.math.log(next_action_probs)
            q1_next_target = self._critic_1(next_state_batch)
            q2_next_target = self._critic_2(next_state_batch)
            min_q = next_action_probs * (
                tf.minimum(q1_next_target, q2_next_target) - self.alpha * next_action_logprobs)
            q_target = reward_batch + self.REWARD_DISCOUNT * min_q

            critic_losses = []
            critic_tapes = []
            for q_net in [self._critic_1, self._critic_2]:
                with tf.GradientTape() as critic_tape:
                    q_value = tf.gather(q_net(state_batch), tf.cast(action_batch, tf.int64), axis=1)
                    critic_losses.append(0.5 * tf.reduce_mean((q_value - q_target) ** 2))
                critic_tapes.append(critic_tape)
        self._critic_1_optimizer.apply_gradients(
            zip(critic_tapes[0].gradient(critic_losses[0], self._critic_1_variables), self._critic_1_variables))
        self._critic_2_optimizer.apply_gradients(
            zip(critic_tapes[1].gradient(critic_losses[1], self._critic_2_variables), self._critic_2_variables))

        with tf.name_scope('alpha_loss'):
            neg_entropy = tf.reduce_sum(action_logprobs * action_probs, axis=1)
            with tf.GradientTape() as alpha_tape:
                alpha_loss = tf.reduce_mean(-1 * self.log_alpha * (neg_entropy + self.target_entropy))
        self._alpha_optimizer.apply_gradients(
            zip(alpha_tape.gradient(alpha_loss, [self.log_alpha]), [self.log_alpha]))
        self.alpha = tf.exp(self.log_alpha)

        self._copy_target_models()


class SoftActorCriticDiscreteAgent(SpiceAIAgent):
    BATCH_SIZE = 128
    UPDATE_STEPS = 10

    def __init__(self, state_shape: tuple, action_size):
        super().__init__(state_shape, action_size)

        self.model = SACD(state_shape, action_size)
        self.model.compile(loss="mse", optimizer=optimizers.SGD())
        self.buffer = ReplayBuffer(self.BATCH_SIZE)

    def add_experience(self, state, action, reward, next_state):
        self.buffer.store(state, action, reward, next_state)

    def act(self, state):
        action, action_probs = self.model.actor.predict(np.expand_dims(state, 0))
        return action[0], action_probs[0]

    def save(self, path: Path):
        model_name = "model.pb"
        model_path = path / model_name
        with open(path / "meta.json", "w", encoding="utf-8") as meta_file:
            meta_file.write(json.dumps({"algorithm": "sacd", "model_name": model_name}))
        self.model.actor.save(model_path)

    def load(self, path: Path) -> bool:
        if (path / "meta.json").exists():
            with open(path / "meta.json", "r", encoding="utf-8") as meta_file:
                meta_info = json.loads(meta_file.read())
            self.model.actor = keras.models.load_model(str(path / meta_info["model_name"]))
            return True
        return False

    def learn(self):
        if self.buffer.size() < self.BATCH_SIZE:
            return

        for _ in range(self.UPDATE_STEPS):
            self.model.train(self.buffer.sample())
