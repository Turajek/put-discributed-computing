import MPI from "mpi-node";

export default async function handlePurchaser(initData) {
  let liftLocation = {};
  let queue = {};
  let liftCritical = {};
  let ackCounter = 0;

  initState();
  handleLiftRequest();
  handleLiftAckRequest();
  handleReleaseRequest();
  handleReleaseOthersRequest();

  await sleep(1000);

  broadcastLift();

  function initState() {
    for (let i = 1; i <= initData.liftsNumber; i++) {
      liftLocation[i] = "UP_ORDERING";
    }
    for (let i = 1; i <= initData.liftsNumber; i++) {
      queue[i] = [];
    }
    for (let i = 1; i <= initData.liftsNumber; i++) {
      liftCritical[i] = null;
    }
  }

  function handleLiftRequest() {
    MPI.recv("LIFT", (msg) => {
      Object.keys(queue).forEach((key) => {
        queue[key].push({ tid: msg.tid, timestamp: msg.timestamp });
        queue[key] = queue[key].sort((a, b) => a.timestamp - b.timestamp);
      });
      MPI.send(msg.tid, { type: "LIFT_ACK" });
    });
  }

  function handleLiftAckRequest() {
    MPI.recv("LIFT_ACK", async (msg) => {
      ackCounter++;
      if (ackCounter == initData.size) {
        ackCounter = 0;
        tryAccessLift();
      }
    });
  }

  function handleReleaseOthersRequest() {
    MPI.recv("LIFT_RELEASE_OTHERS", (msg) => {
      console.log("BEFORE: ");
      console.log(queue);
      const keysToRelease = Object.keys(queue).filter(
        (key) => key != msg.busyKey
      );
      keysToRelease.forEach((key) => {
        queue[key] = queue[key].filter((el) => el.tid != msg.processTid);
      });
      console.log("AFTER: ");
      console.log(queue);
    });
  }

  function handleReleaseRequest() {
    MPI.recv("LIFT_RELEASE", (msg) => {
      console.log("BEFORE: ");
      console.log(queue);
      queue[msg.key] = queue[msg.key].filter((el) => el.tid != msg.processTid);
      console.log("AFTER: ");
      console.log(queue);
    });
  }

  function broadcastLift() {
    MPI.broadcast({
      type: "LIFT",
      tid: initData.tid,
      timestamp: getTimestamp(),
    });
  }

  async function tryAccessLift() {
    const key = getFreeLiftKey();
    if (key) {
      console.log(`-----> LIFT_${key}: tid ${initData.tid}`);
      MPI.broadcast({
        type: "LIFT_RELEASE_OTHERS",
        busyKey: key,
        processTid: initData.tid,
      });
      await sleep(3000);
      MPI.broadcast({
        type: "LIFT_RELEASE",
        key,
        processTid: initData.tid,
      });
    }
  }

  function getFreeLiftKey() {
    for (const key of Object.keys(liftCritical)) {
      if (
        !liftCritical[key] &&
        liftLocation[key] === "UP_ORDERING" &&
        queue[key][0].tid == initData.tid
      ) {
        return key;
      }
    }
    return null;
  }
  function getTimestamp() {
    const hrTime = process.hrtime();
    return hrTime[0] * 1000000 + hrTime[1] / 1000;
  }
  async function sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}
