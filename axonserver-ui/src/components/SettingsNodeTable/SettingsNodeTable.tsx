import React from 'react';
import { Typography } from '../Typography/Typography';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableHead } from '../TableHead/TableHead';
import { TableRow } from '../TableRow/TableRow';
import type { GetPublicResponse } from 'src/services/public/public';

export const SettingsNodeTable = (props: { nodeArray: GetPublicResponse }) => (
  <Table flat>
    <TableHead>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          Node Name
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          Host Name
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          HTTP Port
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          GRPC Port
        </Typography>
      </TableCell>
    </TableHead>

    <TableBody>
      {props.nodeArray.map((nodeItem, index) => (
        <TableRow key={`nodeItem-${index}`}>
          <TableCell>
            <Typography size="m" color="gray">
              {nodeItem.name}
            </Typography>
          </TableCell>
          <TableCell>
            <Typography size="m" color="gray">
              {nodeItem.hostName}
            </Typography>
          </TableCell>
          <TableCell>
            <Typography size="m" color="gray">
              {nodeItem.httpPort}
            </Typography>
          </TableCell>
          <TableCell>
            <Typography size="m" color="gray">
              {nodeItem.grpcPort}
            </Typography>
          </TableCell>
        </TableRow>
      ))}
    </TableBody>
  </Table>
);
