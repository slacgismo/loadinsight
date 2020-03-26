import React from 'react';
import useSWR from "@zeit/swr";
import ClipLoader from 'react-spinners/ClipLoader';
import authedAxios from "./authedAxios"

export default ({src, ...other}) => {
  const { data: dataURL, error } = useSWR(
    src,
     async (key) => {
      const response = await authedAxios.get(key, {responseType: 'blob'});
      const imageBlob = response.data;
      const reader = new FileReader();
      return new Promise((resolve, reject) => {
        reader.onloadend = () => {
          resolve(reader.result)};
          reader.readAsDataURL(imageBlob);
      });
    },
    {
      revalidateOnFocus: false,
      refreshWhenHidden: false,
      shouldRetryOnError: false
    }
  );
  if (error) return <div>{error}</div>;
  if (!dataURL) return <ClipLoader/>;

  return <img src={dataURL} {...other} alt=''/>;
  }
