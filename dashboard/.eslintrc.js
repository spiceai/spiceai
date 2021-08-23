module.exports = {
  root: true,
  parser: "@typescript-eslint/parser",
  plugins: [
    "@typescript-eslint",
    "react", // eslint-plugin-react
    "prettier" // eslint-plugin-prettier
  ],
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended",
    "prettier", // eslint-config-prettier
  ],
  rules: {
    "@typescript-eslint/no-explicit-any": [
      "off"
    ]
  }
};