import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';

test('renders docs header', () => {
  render(<App />);
  const linkElement = screen.getByText(/docs/i);
  expect(linkElement).toBeInTheDocument();
});

test('renders version', () => {
  render(<App />);
  const linkElement = screen.getByText(/v0.1.0-alpha/i);
  expect(linkElement).toBeInTheDocument();
});

test('renders github.com/spiceai/spiceai footer', () => {
  render(<App />);
  const linkElement = screen.getByText(/spiceai\.org/i);
  expect(linkElement).toBeInTheDocument();
});
