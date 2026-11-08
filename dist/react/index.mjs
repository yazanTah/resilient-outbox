// src/react/index.ts
import { useSyncExternalStore } from "react";
function useOutbox(queue) {
  const snapshot = useSyncExternalStore(
    (onStoreChange) => queue.subscribe(onStoreChange),
    () => queue.getSnapshot(),
    () => queue.getSnapshot()
  );
  return {
    ...snapshot,
    flush: () => queue.flush(),
    retryDeadLetter: (id) => queue.retryDeadLetter(id),
    retryAllDeadLetter: () => queue.retryAllDeadLetter(),
    getPending: () => queue.getPending(),
    getDeadLetter: () => queue.getDeadLetter()
  };
}
export {
  useOutbox
};
//# sourceMappingURL=index.mjs.map