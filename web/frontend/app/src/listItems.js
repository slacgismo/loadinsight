import React from 'react';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import ListSubheader from '@material-ui/core/ListSubheader';
import BarChartIcon from '@material-ui/icons/BarChart';
import PlayCircleFilledIcon from '@material-ui/icons/PlayCircleFilled';
import axios from 'axios';

export const mainListItems = (
  <div>
    <ListItem button>
      <ListItemIcon>
        <BarChartIcon />
      </ListItemIcon>
      <ListItemText primary="History" />
    </ListItem>
  </div>
);

function onClick (key) {
  const token = localStorage.getItem('auth_token');
  axios.get(`api/execute/${key}/`, {headers: {'Authorization': `Token ${token}`}}).then(response => {alert('Started successfully!')}).catch(error => alert(error));
}

const algorithms = ['rbsa', 'ceus', 'mix'];

export const secondaryListItems = (
  <div>
    <ListSubheader inset>Execute Algorithms</ListSubheader>
    {algorithms.map((item) =>
      <ListItem button key={item} onClick={() => {onClick(item)}}>
      <ListItemIcon>
        <PlayCircleFilledIcon />
      </ListItemIcon>
      <ListItemText primary={item} />
    </ListItem>)}
  </div>
);