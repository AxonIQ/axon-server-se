import { default as MUiTableCell } from '@material-ui/core/TableCell';
import classnames from 'classnames';
import React from 'react';
import './table-cell.scss';

type TableCellProps = {
  children: React.ReactNode;
  smallWidth?: boolean;
  colSpan?: number;
  overflowElipsis?: boolean;
  align?: 'center' | 'inherit' | 'justify' | 'left' | 'right';
};
export const TableCell = (props: TableCellProps) => (
  <MUiTableCell
    classes={{ root: 'table-cell' }}
    className={classnames(
      props.smallWidth && 'table-cell--small-width',
      props.overflowElipsis && 'table-cell--overflow-elipsis',
    )}
    colSpan={props.colSpan}
    align={props.align}
  >
    {props.children}
  </MUiTableCell>
);
