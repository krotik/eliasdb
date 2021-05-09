module.exports = {
    env: {
        browser: true,
        es2021: true
    },
    extends: ['standard'],
    parser: '@typescript-eslint/parser',
    parserOptions: {
        ecmaVersion: 12,
        sourceType: 'module'
    },
    plugins: ['@typescript-eslint'],
    rules: {
        eqeqeq: [2, 'allow-null'],
        indent: [2, 4],
        quotes: [2, 'single'],
        semi: [2, 'always'],
        'no-console': 0
    }
};
