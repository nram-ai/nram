import { useEffect, useState } from "react";

// Delays propagation of a value until it has been stable for `delay` ms.
// Common use: throttle network writes from a fast-changing input (search
// box, slider drag) without dropping intermediate user state.
export function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(id);
  }, [value, delay]);
  return debounced;
}
