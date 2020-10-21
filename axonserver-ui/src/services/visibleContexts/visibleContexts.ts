import { fetchWrapper } from '../fetchWrapper';

export const visibleContextsUrl = '/v1/public/visiblecontexts';

export type GetVisibleContextsResponse = string[];
export async function getVisibleContexts(
  includeAdmin?: boolean,
): Promise<GetVisibleContextsResponse> {
  const searchParams = new URLSearchParams();
  searchParams.set('includeAdmin', includeAdmin ? 'true' : 'false');

  const response = await fetchWrapper.get(
    `${visibleContextsUrl}?${searchParams.toString()}`,
  );
  return response.json();
}
