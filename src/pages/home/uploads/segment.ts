import { password } from "~/store"
import { EmptyResp } from "~/types"
import { r } from "~/utils"
import sparkMd5 from "spark-md5"
import { SetUpload, Upload } from "./types"
import { blob } from "stream/consumers"
import { pathJoin } from "~/utils"
import { resolve } from "path"
import {Mutex, MutexInterface, Semaphore, SemaphoreInterface, withTimeout} from 'async-mutex';

const mutex = new Mutex();

const getFileMD5 = (file: File):Promise<{hash: String, chunkList: Blob[]}> => {
  return new Promise((resolve, reject) => {
    let blobSlice = File.prototype.slice,
      chunkSize = 1024 * 1024 * 90,
      chunks = Math.ceil(file.size / chunkSize),
      currentChunk = 0,
      spark = new sparkMd5.ArrayBuffer();

    let fileReader = new FileReader();
    let chunkList:Blob[] = [];
    
    fileReader.onerror = reject;
    fileReader.onload = (e) => {
      spark.append(e.target?.result);
      currentChunk++;
      // 上传进度的计算
      if (currentChunk < chunks) {
        loadNext();
      } else {
      // 返回文件MD5
        resolve({hash: spark.end(), chunkList: chunkList});
      }
    };
    function loadNext() {
      let start = currentChunk * chunkSize,
        end = start + chunkSize >= file.size ? file.size : start + chunkSize;

      // chunkList.push()  
      let chunk = blobSlice.call(file, start, end);
      chunkList.push(chunk)
      fileReader.readAsArrayBuffer(chunk);
    }
    // 执行文件切割 计算
    loadNext();
  })
};

const md5Hash = (chunks: Blob[]): Promise<String|null> =>  {
  return new Promise((resolve, reject) => {
    const spark = new sparkMd5.ArrayBuffer();
    for (let chunk of chunks) {
      const reader = new FileReader()
      reader.onload = (e) => {
        spark.append(e.target?.result)
      }

      reader.onerror = (e)=>{
        reject(e)
      }

      reader.readAsArrayBuffer(chunk)
    }
    resolve(spark.end());
  });
}

const chunkFile = (file: File, size: number): Blob[] => {
  const fileSlice = File.prototype.slice;
  const chunk_size = Math.ceil(file.size / size)
  const chunkList = []
  let idx = 0
  while (idx < chunk_size) {
    const start = idx * chunk_size
    const end = Math.min(file.size, start + chunk_size)
    const chunk = fileSlice.call(file, start, end)
    chunkList.push(chunk)
    idx++
  }
  return chunkList
}

const chunkPromise = (chunkTask: any, setUpload: SetUpload) => {
  let oldTimestamp = new Date().valueOf()
  let oldLoaded = 0
  console.log(`upload chunk: ${chunkTask["Chunk"]}`)
  return r.put(
    `/fs/put_segment/${chunkTask["Hash"]}/${chunkTask["Idx"]}`,
    chunkTask["Chunk"],
    {
      headers: {
        "File-Path": chunkTask["File-Path"],
        "As-Task": chunkTask["As-Task"],
        "Content-Type": chunkTask["Content-Type"],
        "Last-Modified": chunkTask["Last-Modified"],
        Count: chunkTask["Count"],
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
    },
  )
}

export const SegmentUpload: Upload = async (
  uploadPath: string,
  file: File,
  setUpload: SetUpload,
  asTask = false,
): Promise<Error | undefined> => {
  // let chunkList = chunkFile(file, 5 * 1024 * 1024) // 单包90m
  // let hash = await md5Hash(chunkList)
  let {hash, chunkList} = await getFileMD5(file)
  console.log(`hash: ${hash}...`)

  let chunkPromiseList = chunkList.map((chunk, idx) => {
    let _path = uploadPath.slice(0, uploadPath.lastIndexOf("/"))
    let _filename = `.${file.name}.part${idx}`
    let _uploadPath = pathJoin(_path, _filename)
    let uploadTask = {
      file: file
    }
    let chunkTask = {
      "File-Path": encodeURIComponent(_uploadPath),
      "As-Task": asTask,
      "Content-Type": chunk.type || "application/octet-stream",
      "Last-Modified": file.lastModified,
      Idx: idx,
      Count: chunkList.length,
      Hash: hash,
      Chunk: chunk,
      Length: chunk.size,
      Password: password(),
    }

    console.log(`chunk-${idx}, size: ${chunk.size}`);

    return chunkPromise(chunkTask, setUpload)
      .then((resp) => {
        if (resp.status !== 200) {
          throw new Error(resp.data)
        } else {
          return resp
        }
      })
      .catch((err) => {
        console.error(err)
      })
  })

  await Promise.all(chunkPromiseList)
    .then((resp) => {
      // TODO
      setUpload("progress", 1);
      // 这里写接口3 问后端是否合并成功了
      r.get(`/fs/put_segment/${hash}`)
        .then((resp) => {
          console.log(resp)
        })
        .catch((err) => {
          console.error(err.msg)
        })
    })
    .catch((err) => {
      console.error(err.msg)
    })
    
  return
  // delete later
  let task = {
    "File-Path": encodeURIComponent(uploadPath),
    "As-Task": asTask,
    "Content-Type": file.type || "application/octet-stream",
    "Last-Modified": file.lastModified,
    Length: file.size,
    Password: password(),
  }
  let oldTimestamp = new Date().valueOf()
  let oldLoaded = 0
  const resp: EmptyResp = await r.put(`/fs/put_segment/${hash}`, file, {
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
