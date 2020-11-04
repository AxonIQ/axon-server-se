import { default as MUiTableRow } from '@material-ui/core/TableRow';
import classnames from 'classnames';
import React from 'react';
import './table-row.scss';

type TableRowProps = {
  children: React.ReactNode;
  onClick?: (event: React.MouseEvent<HTMLTableRowElement, MouseEvent>) => void;
};
export const TableRow = (props: TableRowProps) => (
  <MUiTableRow
    className={classnames(props.onClick && 'table-row--clickable')}
    onClick={props.onClick}
  >
    {props.children}
  </MUiTableRow>
);
