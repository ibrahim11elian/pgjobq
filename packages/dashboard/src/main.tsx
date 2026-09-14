import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from './App.jsx';
import './styles.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Data is refreshed by the SSE stream and by explicit intervals per view, so
      // refetching on every window focus would add load without adding freshness.
      refetchOnWindowFocus: false,
      staleTime: 2000,
      // Retrying a 401 or a 403 is pointless: the credential will not become valid on
      // its own, and each retry just delays telling the user what is wrong.
      retry: (failureCount, error) => {
        const message = error instanceof Error ? error.message : '';
        if (/401|403|Invalid or expired/i.test(message)) return false;
        return failureCount < 2;
      },
    },
  },
});

const container = document.getElementById('root');
if (container === null) throw new Error('#root not found');

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
