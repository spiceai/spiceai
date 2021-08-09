const SPICE_VERSION = "v0.1"

const title = document.getElementById(
  "title"
);

const appContainer = document.getElementById(
  "app-container"
);

const urlSearchParams = new URLSearchParams(window.location.search);
const params = Object.fromEntries(urlSearchParams.entries());
const podName = params.pod

if (!!podName) {
  title.innerText = podName
}

const updateData = async () => { 
  if (!!podName) {
    return fetchAndDisplayFlights()
  }

  return fetchAndDisplayPods()
};

const fetchAndDisplayPods = async () => {
  const response = await fetch(
    `/api/${SPICE_VERSION}/pods`
  );
  if (response.ok) {
    const pods = await response.json();

    appContainer.innerHTML = "<h2>Pods</h2>"

    if (!pods || !pods.length) {
      appContainer.innerText = "No Pods found."
      return
    }

    for (const pod of pods) {
      const podElement = document.createElement("div");
      const podTitle = document.createElement("h3")
      podTitle.innerHTML = `<a href="/?pod=${pod.name}">${pod.name}</a>`
      podElement.appendChild(podTitle)
      const podDetails = document.createElement("p")
      podDetails.innerText = `Manifest Path: ${pod.manifest_path}`
      podElement.appendChild(podDetails)

      appContainer.appendChild(podElement)
    }
  }
}

let chart;
let lastEpisode = -1;

const fetchAndDisplayFlights = async() => {
  let canvasElement = document.getElementById('flights-chart')
  if (!chart) {
    appContainer.innerHTML = `<h2>${podName} flights</h2>`
    chartContainer = document.createElement('div')
    chartContainer.height = 500
    canvasElement = document.createElement('canvas')
    canvasElement.id = 'flights-chart'    
    chartContainer.appendChild(canvasElement)
    appContainer.appendChild(chartContainer)
    const ctx = canvasElement.getContext('2d');
    chart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: "Score",
          data: [],
          borderColor: [
              'rgba(00, 22, 132, 1)',
          ],
        }]
      }
    })
  }

  const response = await fetch(
    `/api/v0.1/pods/${podName}/flights`
  );

  if (response.ok) {
    const flights = await response.json();
    for (const flight of flights) {
      for (const ep of flight.episodes) {
        if (ep.episode <= lastEpisode) {
          continue
        }
        chart.data.labels.push(ep["episode"]);
        chart.data.datasets.forEach((dataset) => {
          dataset.data.push(ep["score"])
        });
        chart.update()
        lastEpisode = ep.episode
      }
    }
  }
}

setInterval(() => {
  updateData();
}, 250);
