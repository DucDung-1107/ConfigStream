export const count = (value: number) =>
  new Intl.NumberFormat("en-US").format(value);
export const seconds = (value: number) => `${value.toFixed(3)} s`;
export function bytes(value: number) {
  return `${count(value)} bytes (${(value / 1024 / 1024).toFixed(2)} MiB)`;
}
export const percentSaving = (baseline: number, candidate: number) =>
  (1 - candidate / baseline) * 100;
export function sourceUrl(commit: string, path: string) {
  if (
    !/^[a-f0-9]{40}$/.test(commit) ||
    !/^(workloads\/results\/[a-z0-9-]+\/[a-z0-9-]+\.json|tests\/fixtures\/phase16\/mrt\.json)$/.test(
      path,
    )
  )
    throw new Error("Unapproved evidence link");
  return `https://github.com/kongwoang/ConfigStream/blob/${commit}/${path}`;
}
