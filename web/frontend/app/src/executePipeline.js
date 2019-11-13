import React from 'react';
import Title from './title';
import useSWR, {mutate} from "@zeit/swr";
import authedAxios from "./authedAxios";
import ReactJson from 'react-json-view';
import Button from "@material-ui/core/Button"


export default function ExecutePipeline(props) {
  const key = `/api/executions/configs/`;
  const { data, error } = useSWR(
    key,
    async (key) => {
      const response = await authedAxios.get(key);
      return response.data;
    },
    {
      revalidateOnFocus: false,
      refreshWhenHidden: false,
      shouldRetryOnError: false
    }
  );


  function onClick() {
    authedAxios.post(`/api/executions/`,
      {configs: data, pipeline_name: props.match.params.pipeline_name}).then(response => {alert('Started successfully!')}).catch(error => alert(error));
  }

  return (
    <React.Fragment>
      <Title>Configure {props.match.params.pipeline_name}
        <Button
          variant="contained"
          color="primary"
          style={{float: 'right'}}
          onClick={() => {onClick()}}
        >
          Execute
        </Button>
      </Title>
      {data && <ReactJson src={data}
                          displayObjectSize={false}
                          displayDataTypes={false}
                          onEdit={({updated_src})=>{mutate(key, updated_src, false)}}
                          collapsed={1}/>}
    </React.Fragment>
  );
}