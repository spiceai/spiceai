module.exports = {
  content: ['src/**/*.js', 'src/**/*.jsx', 'src/**/*.ts', 'src/**/*.tsx', 'public/**/*.html'],
  theme: {
    extend: {
      colors: {
        primary: {
          DEFAULT: '#FA7500',
          dark: '#022132',
        },
        secondary: {
          light: '#F8F8F8',
          DEFAULT: '#D2D2D2',
          dark: '#999999',
        },
        cloud: '#e4e9fd',
        sidebar: {
          DEFAULT: '#3D3F45',
          counter: 'rgba(196, 196, 196, 0.39)',
          'gradient-start': 'rgba(49, 94, 182, 1.0)',
          'gradient-end': 'rgba(47, 74, 156, 1.0)',
        },
        body: '#393939',
        footer: '#999999',
        link: '#3059B1',
        search: {
          DEFAULT: 'rgba(229, 234, 254, 0.8)',
          hover: 'rgba(229, 234, 254, 0.85)',
        },
        header: {
          DEFAULT: '#393939',
          bg: '#9978E2',
        },
        label: {
          highlight: '#FFEC41',
        },
        code: {
          DEFAULT: 'rgb(39, 40, 34)',
        },
      },
      letterSpacing: {
        spice: '.48em',
      },
      minWidth: {
        sidebar: '220px',
      },
      height: {
        120: '30rem',
      },
    },
  },
  plugins: [],
}
