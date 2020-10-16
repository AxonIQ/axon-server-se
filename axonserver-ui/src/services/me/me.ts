export const meUrl = '/v1/public/me';

export type GetMeResponse = {
  adminNode: boolean;
  authentication: boolean;
  clustered: boolean;
  contextNames: string[];
  developmentMode: boolean;
  grpcInternalPort: number;
  grpcPort: number;
  hostName: string;
  httpPort: number;
  internalHostName: null | string;
  name: string;
  ssl: boolean;
  storageContextNames: string[];
};
export async function getMe(): Promise<GetMeResponse> {
  const response = await fetch(meUrl);
  return response.json();
}
