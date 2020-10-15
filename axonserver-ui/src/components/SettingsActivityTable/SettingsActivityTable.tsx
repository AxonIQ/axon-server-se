import React from 'react';
import { Typography } from '../Typography/Typography';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableHead } from '../TableHead/TableHead';
import { TableRow } from '../TableRow/TableRow';

export const SettingsActivityTable = () => (
  <Table flat>
    <TableHead>
      <TableCell colSpan={2}>
        <Typography size="m" color="white" weight="bold" uppercase>
          Activity In The Last Minute
        </Typography>
      </TableCell>
    </TableHead>

    <TableBody>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Commands received / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Queries received / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Events stored / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Snapshots stored / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);
