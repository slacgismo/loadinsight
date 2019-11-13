import React from 'react';
import Grid from '@material-ui/core/Grid';
import AuthImg from "./AuthImg";
import useSWR from "@zeit/swr";
import authedAxios from "./authedAxios"


export default function Images(props) {
  const key = `/api/executions/${props.match.params.execution_id}/results/${props.match.params.result_dir}/`;
  const { data, error } = useSWR(
    key,
    async (key) => {
      const image_list = await authedAxios.get(key);
      return image_list.data.images;
    },
    {
      revalidateOnFocus: false,
      refreshWhenHidden: false,
      shouldRetryOnError: false
    }
  );

  return (
    data &&
    <Grid container spacing={1}>
      {[...Array(Math.floor(data.length/3))].map((_, rowIndex) => (
        <Grid container item xs={12} spacing={3}>
          <React.Fragment>
            {[...Array(3)].map((_, colIndex) => (
              (rowIndex * 3 + colIndex) < data.length &&
              <Grid item xs={4}>
                <AuthImg
                  style={{width: '100%', height: 'auto'}}
                  src={`${key}images/${data[rowIndex * 3 + colIndex]}/`}/>
              </Grid>
            ))}
          </React.Fragment>
        </Grid>)
      )}
    </Grid>);
}