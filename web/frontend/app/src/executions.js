import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Title from './title';
import useSWR from "@zeit/swr";


export default function Executions(props) {
  const token = localStorage.getItem('auth_token');
  const { data, error } = useSWR(
    '/api/executions/',
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

  return (
    <React.Fragment>
      <Title>Recent Executions</Title>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Id</TableCell>
            <TableCell>Started Time</TableCell>
            <TableCell>Algorithm</TableCell>
            <TableCell align="right">Status</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {data &&
          data.map(row => (
            <TableRow key={row.id} hover={true} onClick={() => {onClick(row.id)}}>
              <TableCell>{row.id}</TableCell>
              <TableCell>{String(row.time).split('T').map(str => str.split('.')[0]).join(' ')}</TableCell>
              <TableCell>{row.algorithm}</TableCell>
              <TableCell align="right">{row.status}</TableCell>
            </TableRow>
          ))
          }
        </TableBody>
      </Table>
    </React.Fragment>
  );
}