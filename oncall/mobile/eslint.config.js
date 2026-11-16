const { defineConfig, globalIgnores } = require("eslint/config");
const expoConfig = require("eslint-config-expo/flat");

module.exports = defineConfig([
  globalIgnores(["dist/*", "web-build/*", "node_modules/*", ".expo/*", "playwright-report/*", "test-results/*"]),
  expoConfig,
  {
    files: ["**/*.ts", "**/*.tsx"],
    rules: {
      "@typescript-eslint/no-explicit-any": "error",
      "no-console": ["error", { allow: ["warn", "error"] }],
    },
  },
  {
    // jest.mock() calls must precede the imports they affect (babel-plugin-jest-hoist), and mock factories are anonymous components
    files: ["__tests__/**"],
    rules: { "import/first": "off", "react/display-name": "off" },
  },
]);
