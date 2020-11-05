import React, { useEffect, useState } from 'react';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableFooter } from '../TableFooter/TableFooter';
import { TableHead } from '../TableHead/TableHead';
import { TablePagination } from '../TablePagination/TablePagination';
import { TablePaginationActions } from '../TablePaginationActions/TablePaginationActions';
import { TableRow } from '../TableRow/TableRow';
import { Typography } from '../Typography/Typography';

export type SearchRowItem = {
  idValues: number[];
  sortValues: number[];
  value: {
    [key: string]: any;
  };
};
type SearchResultTableProps = {
  headers: string[];
  data: SearchRowItem[];
  onRowClick?: (rowItem: SearchRowItem) => void;
};
export const SearchResultTable = (props: SearchResultTableProps) => {
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [currentPage, setCurrentPage] = useState(0);
  const [headersToShow, setHeadersToShow] = useState<string[]>(props.headers);
  const [rowsToShow, setRowsToShow] = useState<SearchRowItem[]>([]);

  useEffect(() => {
    const firstElemOfPage = rowsPerPage * currentPage;
    const lastElemOfPage = firstElemOfPage + rowsPerPage;
    setRowsToShow(props.data.slice(firstElemOfPage, lastElemOfPage));
  }, [props.data, rowsPerPage, currentPage]);

  return (
    <Table fixedLayout flat>
      <TableHead>
        {headersToShow.map((header, index) => (
          <TableCell key={`search-result-header-${index}`}>
            <Typography size="m" color="white" weight="bold">
              {header}
            </Typography>
          </TableCell>
        ))}
      </TableHead>

      <TableBody>
        {rowsToShow.map((item, index) => (
          <TableRow
            onClick={() => props.onRowClick && props.onRowClick(item)}
            key={`search-result-data-row-${index}`}
          >
            {headersToShow.map((header, itemIndex) => (
              <TableCell
                key={`search-result-data-item-${itemIndex}`}
                overflowElipsis
              >
                <Typography noBreak inline size="m" color="light">
                  {item.value[header]}
                </Typography>
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
      <TableFooter>
        <TableRow>
          <TablePagination
            rowsPerPageOptions={[10, 25, 50, { label: 'All', value: -1 }]}
            colSpan={headersToShow.length}
            count={props.data.length}
            rowsPerPage={rowsPerPage}
            page={currentPage}
            SelectProps={{
              inputProps: { 'aria-label': 'rows per page' },
              native: true,
            }}
            onChangePage={(_, newPage) => setCurrentPage(newPage)}
            onChangeRowsPerPage={(event) => {
              setRowsPerPage(parseInt(event.target.value, 10));
              setCurrentPage(0);
            }}
            ActionsComponent={TablePaginationActions}
          />
        </TableRow>
      </TableFooter>
    </Table>
  );
};
