import React from 'react';
import { Typography } from '../Typography/Typography';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableHead } from '../TableHead/TableHead';
import { TableRow } from '../TableRow/TableRow';

export const SettingsNodeTable = () => (
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
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-1
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-1
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8024
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8124
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-2
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-2
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8025
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8125
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-3
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-3
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8026
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8126
          </Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);
