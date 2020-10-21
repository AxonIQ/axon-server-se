module.exports = {
  stories: ['../src/**/*.stories.mdx', '../src/**/*.stories.@(js|jsx|ts|tsx)'],
  addons: [
    '@storybook/addon-links',
    '@storybook/addon-essentials',
    '@storybook/preset-scss',
    'storybook-dark-mode/register',
  ],
  webpackFinal: (config) => {
    config.module.rules.push({
      test: /\.[tj]sx?$/,
      loader: [
        // This assumes snowpack@>=2.9.0
        require.resolve('@open-wc/webpack-import-meta-loader'),
        require.resolve(
          '@snowpack/plugin-webpack/plugins/proxy-import-resolve',
        ),
      ],
    });
    return config;
  },
};
