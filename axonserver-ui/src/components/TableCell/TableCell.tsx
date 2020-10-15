import React from 'react';
import { default as MUiTableCell } from '@material-ui/core/TableCell';
import './table-cell.scss';
import classnames from 'classnames';

type TableCellProps = {
  children: React.ReactNode;
  smallWidth?: boolean;
  colSpan?: number;
  align?: 'center' | 'inherit' | 'justify' | 'left' | 'right';
};
export const TableCell = (props: TableCellProps) => (
  <MUiTableCell
    classes={{ root: 'table-cell' }}
    className={classnames(props.smallWidth && 'table-cell--small-width')}
    colSpan={props.colSpan}
    align={props.align}
  >
    {props.children}
  </MUiTableCell>
);
