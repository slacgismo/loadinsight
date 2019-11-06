/* eslint-disable no-script-url */

import React from 'react';
import Link from '@material-ui/core/Link';
import { makeStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Title from './title';
import useSWR from "@zeit/swr";

const useStyles = makeStyles(theme => ({
  seeMore: {
    marginTop: theme.spacing(3),
  },
}));

export default function Executions(props) {
  const token = localStorage.getItem('auth_token');
  const { data, error } = useSWR(
    '/api/my_executions/',
    async (key) => {
      const response = await fetch(key, {
        headers: new Headers([['Authorization', `Token ${token}`]])});
      return await response.json();
    },
    {
      revalidateOnFocus: false,
      refreshWhenHidden: false,
      shouldRetryOnError: false
    }
  );


  function onClick(execution_id) {
    props.history.push(`/dashboard/executions/${execution_id}`);
  }


  const classes = useStyles();
  return (
    <React.Fragment>
      <Title>Recent Executions</Title>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Id</TableCell>
            <TableCell>Started Time</TableCell>
            <TableCell>Algorithm</TableCell>
            {/*<TableCell align="right">Status</TableCell>*/}
          </TableRow>
        </TableHead>
        <TableBody>
          {data &&
          data.map(row => (
            <TableRow key={row.id} hover={true} onClick={() => {onClick(row.id)}}>
              <TableCell>{row.id}</TableCell>
              <TableCell>{String(row.create_time).split('T').map(str => str.split('.')[0]).join(' ')}</TableCell>
              <TableCell>{row.algorithm}</TableCell>
              {/*<TableCell align="right">{row.shipTo}</TableCell>*/}
            </TableRow>
          ))
          }
        </TableBody>
      </Table>
      <div className={classes.seeMore}>
        <Link color="primary" href="">
          See more executions
        </Link>
      </div>
    </React.Fragment>
  );
}