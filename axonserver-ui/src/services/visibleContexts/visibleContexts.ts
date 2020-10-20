export const visibleContextsUrl =
  '/v1/public/visiblecontexts?includeAdmin=false';

export type GetVisibleContextsResponse = string[];
export async function getVisibleContexts(): Promise<
  GetVisibleContextsResponse
> {
  const response = await fetch(visibleContextsUrl);
  return response.json();
}
