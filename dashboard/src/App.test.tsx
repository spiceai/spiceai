import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';
import { readFileSync } from 'fs';

test('renders docs header', () => {
  render(<App />);
  const linkElement = screen.getByText(/docs/i);
  expect(linkElement).toBeInTheDocument();
});

test('renders version', () => {
  const versionFile = readFileSync("../version.txt")
  const versionString = `v${versionFile.toString().trim()}`
  console.log(`renders version test: testing for version '${versionString}'`)
  render(<App />);
  const linkElement = screen.getByText(new RegExp(versionString, "i"));
  expect(linkElement).toBeInTheDocument();
});

test('renders github.com/spiceai/spiceai footer', () => {
  render(<App />);
  const linkElement = screen.getByText(/spiceai\.org/i);
  expect(linkElement).toBeInTheDocument();
});
