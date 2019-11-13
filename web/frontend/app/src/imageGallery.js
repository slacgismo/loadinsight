import React, {useState} from "react";
import {Field, Form, Formik} from "formik"
import {TextField} from "formik-material-ui"
import Button from "@material-ui/core/Button"
import authedAxios from "./authedAxios";
import { makeStyles } from '@material-ui/core/styles';
import Grid from "@material-ui/core/Grid"
import AuthImg from "./AuthImg"


const useStyles = makeStyles(theme => ({
  textField: {
    marginLeft: theme.spacing(1),
    marginRight: theme.spacing(1),
    width: 200,
  },
}));

const getTrimmedString = (str) => {
  if (str === null || str === undefined) {
    return null;
  }

  str = str.trim();
  if (str.length === 0) {
    return null;
  }

  return str;
}

export default function ImageGallery(props) {
  const classes = useStyles();
  const [imageUrls, setImageUrls] = useState(undefined);
  const apiPath = `/api/executions/${props.match.params.execution_id}/results/${props.match.params.result_dir}/`;

  return (
    <React.Fragment>
    <Formik
      initialValues={{
        city_name: '',
        state_name: '',
        content_name: ''
      }}
      onSubmit={({city_name, state_name, content_name}, actions) => {
        authedAxios.get(apiPath, {
          params: {
            city_name: getTrimmedString(city_name),
            state_name: getTrimmedString(state_name),
            content_name: getTrimmedString(content_name)
          }
        }).then(response => {
          setImageUrls(response.data.images);
        }).catch(error => {
          setImageUrls(undefined);
        });
      }}
    >
      {({errors, status, touched, isSubmitting}) =>
        (
          <Form>
          <Field
            type="text"
            name="city_name"
            label="City"
            component={TextField}
            className={classes.textField}
            autoComplete="city_name"
            variant="outlined"
            id="city_name"
          />
          <Field
            type="text"
            name="state_name"
            label="State"
            component={TextField}
            className={classes.textField}
            autoComplete="state_name"
            variant="outlined"
            id="state_name"
          />
          <Field
            type="text"
            name="content_name"
            label="Content"
            component={TextField}
            className={classes.textField}
            autoComplete="content_name"
            variant="outlined"
            id="content_name"
          />
          <Button
            type="submit"
            variant="contained"
            color="primary"
            style={{float: 'right'}}
          >
            Search
          </Button>
        </Form>)}
    </Formik>
      {imageUrls &&
      <Grid container spacing={1}>
        {[...Array(Math.floor(imageUrls.length/3))].map((_, rowIndex) => (
          <Grid container item xs={12} spacing={3}>
            <React.Fragment>
              {[...Array(3)].map((_, colIndex) => (
                (rowIndex * 3 + colIndex) < imageUrls.length &&
                <Grid item xs={4}>
                  <AuthImg
                    style={{width: '100%', height: 'auto'}}
                    src={`${apiPath}images/${imageUrls[rowIndex * 3 + colIndex]}/`}/>
                </Grid>
              ))}
            </React.Fragment>
          </Grid>)
        )}
      </Grid>
      }
    </React.Fragment>
  );

}