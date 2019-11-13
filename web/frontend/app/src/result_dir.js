/* eslint-disable no-script-url */

import React from 'react';
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

export default function ResultDirs(props) {
  const token = localStorage.getItem('auth_token');
  const { data, error } = useSWR(
    `/api/executions/${props.match.params.execution_id}/`,
    async (key) => {
      const response = await fetch(key, {
        headers: new Headers([['Authorization', `Token ${token}`]])});
      const json = await response.json();
      return json.execution_dirs;
    },
    {
      revalidateOnFocus: false,
      refreshWhenHidden: false,
      shouldRetryOnError: false
    }
  );


  function onClick(resultDir) {
    props.history.push(`/dashboard/executions/${props.match.params.execution_id}/results/${resultDir}`);
  }

  const classes = useStyles();
  return (
    <React.Fragment>
      <Title>Execution Results</Title>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Name</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {data &&
          data.map(row => (
            <TableRow hover={true} onClick={() => {onClick(row)}}>
              <TableCell>{row}</TableCell>
            </TableRow>
          ))
          }
        </TableBody>
      </Table>
    </React.Fragment>
  );
}