import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  base: '/voice-chat/',
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/voice-chat/api': {
        target: 'http://localhost:3000',
        rewrite: (path) => path.replace(/^\/voice-chat/, ''),
      },
    },
  },
});
