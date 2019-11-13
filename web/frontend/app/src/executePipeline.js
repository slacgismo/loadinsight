import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Title from './title';
import useSWR from "@zeit/swr";
import authedAxios from "./authedAxios";


export default function ExecutePipeline(props) {
  const { data, error } = useSWR(
    `/api/executions/configs/`,
    async (key) => {
      const response = await authedAxios.get(key);
      console.log(response.data);
      return response.data;
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
          {/*{data &&*/}
          {/*data.map(row => (*/}
          {/*  <TableRow hover={true} onClick={() => {onClick(row)}}>*/}
          {/*    <TableCell>{row}</TableCell>*/}
          {/*  </TableRow>*/}
          {/*))*/}
          {/*}*/}
        </TableBody>
      </Table>
    </React.Fragment>
  );
}