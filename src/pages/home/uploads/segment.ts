import { password } from "~/store"
import { EmptyResp } from "~/types"
import { r } from "~/utils"
import sparkMd5 from "spark-md5";
import { SetUpload, Upload } from "./types"
import path from "path";
import { blob } from "stream/consumers";

const md5Hash = (chunks: Blob[]) => {
    const spark = new sparkMd5();
    function _read(i:number) {
      if (i>chunks.length) {
        return spark.end();
      }

      const blob = chunks[i];
      const reader = new FileReader();
      reader.onload = e=>{
        const bytes = e.target?.result||'';
        spark.append(bytes);
        _read(i+1);
      }

      reader.readAsArrayBuffer(blob);
    }

    return _read(0);
}

const chunkFile = (file: File, size: number): Blob[] => {
  const chunk_size = Math.ceil(file.size / size);
  const chunkList = [];
  let idx = 0;
  while (idx<chunk_size) {
    const start = idx*chunk_size
    const end = Math.min(file.size, start+chunk_size)
    const chunk = file.slice(start, end);
    chunkList.push(chunk);
    idx++
  }
  return chunkList;
}

const chunkPromise = (
  chunkTask:any, setUpload: SetUpload) => {
    let oldTimestamp = new Date().valueOf()
    let oldLoaded = 0;
    return r.put("/fs/put_segment", chunkTask["chunk"], {
      headers: {
        "File-Path": chunkTask["File-Path"],
        "As-Task": chunkTask["As-Task"],
        "Content-Type": chunkTask["Content-Type"],
        "Last-Modified": chunkTask["Last-Modified"],
        Password: password(),
      },
      onUploadProgress: (progressEvent) => {
        if (progressEvent.total) {
          const complete =
            ((progressEvent.loaded / progressEvent.total) * 100) | 0
          setUpload("progress", complete)
  
          const timestamp = new Date().valueOf()
          const duration = (timestamp - oldTimestamp) / 1000
          if (duration > 1) {
            const loaded = progressEvent.loaded - oldLoaded
            const speed = loaded / duration
            const remain = progressEvent.total - progressEvent.loaded
            const remainTime = remain / speed
            setUpload("speed", speed)
            console.log(remainTime)
  
            oldTimestamp = timestamp
            oldLoaded = progressEvent.loaded
          }
  
          if (complete === 100) {
            setUpload("status", "backending")
          }
        }
      },
    })
}

export const SegmentUpload: Upload = async (
  uploadPath: string,
  file: File,
  setUpload: SetUpload,
  asTask = false,
): Promise<Error | undefined> => {
  let chunkList = chunkFile(file, 50*1024*1024);
  let hash = md5Hash(chunkList);

  let chunkPromiseList = chunkList.map((chunk,idx)=>{
    let _path = uploadPath.slice(0, uploadPath.lastIndexOf("/"))
    let _filename = `.${file.name}.part${idx}`;
    let _uploadPath = path.join(_path, _filename);
    let chunkTask = {
      "File-Path": encodeURIComponent(_uploadPath),
      "As-Task": asTask,
      "Content-Type": chunk.type || "application/octet-stream",
      "Last-Modified": file.lastModified,
      Chunk: chunk,
      Length: chunk.size,
      Password: password(),
    };

    return chunkPromise(chunkTask, setUpload).then(resp=>{
      if (resp.status !== 200)throw new Error(resp.data)
    }).catch(err=>{
      console.error(err.message)
    });
  });
  
  // delete later
  let task = {
    "File-Path": encodeURIComponent(uploadPath),
    "As-Task": asTask,
    "Content-Type": file.type || "application/octet-stream",
    "Last-Modified": file.lastModified,
    Length: file.size,
    Password: password(),
  };
  let oldTimestamp = new Date().valueOf()
  let oldLoaded = 0
  const resp: EmptyResp = await r.put("/fs/put_segment", file, {
    headers: {
      "File-Path": encodeURIComponent(uploadPath),
      "As-Task": asTask,
      "Content-Type": file.type || "application/octet-stream",
      "Last-Modified": file.lastModified,
      Password: password(),
    },
    onUploadProgress: (progressEvent) => {
      if (progressEvent.total) {
        const complete =
          ((progressEvent.loaded / progressEvent.total) * 100) | 0
        setUpload("progress", complete)

        const timestamp = new Date().valueOf()
        const duration = (timestamp - oldTimestamp) / 1000
        if (duration > 1) {
          const loaded = progressEvent.loaded - oldLoaded
          const speed = loaded / duration
          const remain = progressEvent.total - progressEvent.loaded
          const remainTime = remain / speed
          setUpload("speed", speed)
          console.log(remainTime)

          oldTimestamp = timestamp
          oldLoaded = progressEvent.loaded
        }

        if (complete === 100) {
          setUpload("status", "backending")
        }
      }
    },
  })
  if (resp.code === 200) {
    return
  } else {
    return new Error(resp.message)
  }
}
