import React from 'react';
import { Typography } from '../Typography/Typography';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableHead } from '../TableHead/TableHead';
import { TableRow } from '../TableRow/TableRow';
import FileCopyIcon from '@material-ui/icons/FileCopy';
import IconButton from '@material-ui/core/IconButton';

type SearchRowDetailsProps = {
  dataItem: {
    [key: string]: any;
  };
};
export const SearchRowDetails = (props: SearchRowDetailsProps) => (
  <Table flat>
    <TableHead>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          Name
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          Value
        </Typography>
      </TableCell>
      <TableCell align="center">
        <Typography size="m" color="white" weight="bold" uppercase>
          Copy
        </Typography>
      </TableCell>
    </TableHead>

    <TableBody>
      {Object.entries(props.dataItem).map(
        ([rowItemKey, rowItemValue], index) => (
          <TableRow key={`search-row-details-${index}`}>
            <TableCell>
              <Typography size="m" color="light" weight="bold" uppercase>
                {rowItemKey}
              </Typography>
            </TableCell>
            <TableCell>
              <Typography size="m" color="light">
                {rowItemValue}
              </Typography>
            </TableCell>
            <TableCell align="center">
              <IconButton
                className="search-row-details__copy-button"
                aria-label="copy"
                onClick={() => {
                  const dummy = document.createElement('textarea');
                  document.body.appendChild(dummy);
                  dummy.value = rowItemValue;
                  dummy.select();
                  document.execCommand('copy');
                  document.body.removeChild(dummy);
                }}
              >
                <FileCopyIcon />
              </IconButton>
            </TableCell>
          </TableRow>
        ),
      )}
    </TableBody>
  </Table>
);
