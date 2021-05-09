const path = require('path');

module.exports = {
    mode: 'production',
    entry: './src/frontend.ts',
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
                exclude: /node_modules/
            }
        ]
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js']
    },
    output: {
        filename: 'frontend.js',
        path: path.resolve(__dirname, 'dist'),
        library: 'astro',
        libraryTarget: 'window',
        libraryExport: 'default'
    }
};
