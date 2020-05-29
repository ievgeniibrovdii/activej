class Store {
  constructor(storage, name, defaultValue) {
    this.storage = storage;
    this.name = name;
    this.value = storage.getItem(name) || (typeof defaultValue == 'string' ? defaultValue : JSON.stringify(defaultValue));
  }

  set(value) {
    this.value = value;
    this.storage.setItem(this.name, value);
  }

  setJson(value) {
    this.set(JSON.stringify(value));
  }

  get() {
    return this.value;
  }

  getJson() {
    return JSON.parse(this.value);
  }
}

const coordStores = [];

let viz = null;

function loadPartitions() {
  const $body = $('#body');
  const $partitions = $('#partitions');
  const $tasks = $('#tasks');
  const $partitionView = $('#partition-view');
  const $taskView = $('#task-view');

  $partitions.empty();

  let currentPartitionIdx;

  fetch('/api/partitions')
    .then(r => r.json())
    .then(partitions => {
      for (let i = 0; i < partitions.length; i++) {
        const p = partitions[i];
        const $partitionButton = $(`<button class="list-group-item list-group-item-action">${p}</button>`)
          .click(() => {
            currentPartitionIdx = i;
            $taskView.empty();
            fetch(`/api/partitions/${i}/tasks`)
              .then(r => r.json())
              .then(json => {
                $partitionView.html(`
                  <table class="table table-sm table-borderless">
                    <tr><td>running</td><td>${json.running.length}</td></tr>
                    <tr><td>succeeded</td><td>${json.succeeded}</td></tr>
                    <tr><td>failed</td><td>${json.failed}</td></tr>
                    <tr><td>cancelled</td><td>${json.cancelled}</td></tr>
                  </table>`
                );
                $partitions.children().removeClass('active');
                $partitionButton.addClass('active');

                $tasks.children().remove();
                for (let taskId of json.running) {
                  const $taskButton = $(`<button class="list-group-item list-group-item-action">task #${taskId}</button>`)
                    .click(() => {
                      Promise.all([
                        fetch(`/api/partitions/${i}/tasks/${taskId}/graph`),
                        fetch(`/api/partitions/${i}/tasks/${taskId}/streams`)
                      ])
                        .then(([res1, res2]) => Promise.all([res1.text(), res2.json()]))
                        .then(([gv, streams]) => viz.renderSVGElement(gv).then(svg => [svg, streams]))
                        .then(([svg, streams]) => {
                          const $controlPanel = $('<div class="row my-2"></div>')
                            .append('<button class="btn mr-2 btn-danger" id="cancel-task">Cancel task</button>');
                          const $graphRow = $('<div class="row"></div>')
                          $taskView.empty().append($controlPanel).append($graphRow);

                          let coords = coordStores[[p, taskId]];
                          if (!coords) {
                            coords = new Store(sessionStorage, `coords|${p}|${taskId}`, {x: 0, y: 0, scale: 1});
                            coordStores[[p, taskId]] = coords;
                          }
                          const $svg = setPannableSvg($graphRow, coords, svg);
                          for (const [streamId, bytes] of Object.entries(streams.uploads)) {
                            const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
                            title.textContent = `uploaded: ${formatBytes(bytes)}`;
                            $svg.find(`#s${streamId}`).append(title);
                          }
                          for (const [streamId, bytes] of Object.entries(streams.downloads)) {
                            const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
                            title.textContent = `downloaded: ${formatBytes(bytes)}`;
                            $svg.find(`#s${streamId}`).append(title);
                          }
                        }, e => {
                          viz = new Viz(); // this is needed according to viz documentation
                          console.error(e);
                        });
                    });
                  $tasks.append($taskButton)
                }
              })
          });
        $partitions.append($partitionButton)
      }
      $body.show()
    })
    .catch(console.log);

  let $stopServerButton = $('#stopServerButton');
  $stopServerButton.click(() => {
    if (currentPartitionIdx) {
      fetch(`/api/partitions/${currentPartitionIdx}/stop`, {method: 'POST'})
        .then(() => $stopServerButton.previous().click())
        .catch(console.log)
    }
  })
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 bytes';
  if (bytes === 1) return '1 byte';
  let power = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, power)).toFixed(2) + ' ' + ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'][power];
}

function setPannableSvg($target, coords, svg) {
  const $svg = $(svg);
  $svg.addClass('graph-svg');
  $svg.find('title').remove(); // remove all the titles
  $svg.find('g.graph > polygon').remove(); // remove white background

  const $pos = $('<div></div>');
  const $scale = $('<div></div>');
  const $resetButton = $('<svg style="position: relative; z-index: 1" width="16" height="16">' +
    '<rect x="10" y="6.5" width="5" height="2" fill="#99f"/>' +
    '<rect x="0" y="6.5" width="5" height="2" fill="#99f"/>' +
    '<rect x="6.5" y="0" width="2" height="5" fill="#99f"/>' +
    '<rect x="6.5" y="10.5" width="2" height="5" fill="#99f"/>' +
    '<rect x="6.5" y="6.5" width="2" height="2" fill="#99f"/>' +
    '</svg>');

  $resetButton.click(() => {
    x = 0;
    y = 0;
    scale = 1;
    transform(x, y, scale);
  });
  const $container = $('<div class="graph-container"></div>')
    .append($('<div class="graph-info ml-1"></div>')//.append($pos).append($scale)
      .append($resetButton));

  // need to add this in the middle to get a computed bbox for the next steps
  $target.empty().append($container.append($svg));

  // set the size of svg to match its fixed contents (actual nodes and stuff)
  let bbox = $svg[0].getBBox();
  $svg.attr('width', bbox.width + 2);
  $svg.attr('height', bbox.height + 2);
  $svg.attr('viewBox', [bbox.x - 2, bbox.y - 2, bbox.width + 4, bbox.height + 4].join(' '));

  let containerBBox = $container[0].getBoundingClientRect();

  let baseX, baseY;

  if (bbox.width < containerBBox.width && bbox.height < containerBBox.height) {
    baseX = containerBBox.width / 2 - bbox.width / 2;
    baseY = containerBBox.height / 2 - bbox.height / 2;
  } else {
    let highestBbox = null;
    let kek = null;
    $svg.find('g.node').each((_, n) => {
      let bbox = n.getBBox();
      if (!highestBbox || bbox.y < highestBbox.y) {
        highestBbox = bbox;
        kek = n;
      }
    });
    highestBbox = highestBbox || bbox;
    baseX = -highestBbox.x - highestBbox.width / 2 + containerBBox.width / 2;
    baseY = -highestBbox.y - bbox.height - highestBbox.height / 2 + containerBBox.height / 6;
  }

  let {x, y, scale} = coords.getJson();
  let newX = 0.0, newY = 0.0;
  let startX, startY, dragging = false;

  function transform(x, y, scale) {
    $pos.text(`pos: ${x.toFixed()}:${y.toFixed()}`);
    $scale.text(`scale: ${scale.toFixed(2)}`);
    $svg.css('transform', 'translate(' + (baseX + x) + 'px, ' + (baseY + y) + 'px) scale(' + scale + ')');
    coords.setJson({x, y, scale});
  }

  transform(x, y, scale);

  function mouseup() {
    dragging = false;
    x = newX;
    y = newY;
  }

  $container.mousedown(e => {
    startX = e.pageX;
    startY = e.pageY;
    dragging = true;
  });
  $container.mousemove(e => {
    if (!dragging) {
      return;
    }
    if (e.buttons === 0) {
      mouseup();
      return
    }
    newX = x + e.pageX - startX;
    newY = y + e.pageY - startY;
    transform(newX, newY, scale);
  });
  $container.mouseup(mouseup);
  $container.mousewheel(e => {
    let prevScale = scale;
    let newScale = prevScale;

    if (e.deltaY < 0) {
      if (newScale > 0.1) {
        newScale /= 1.1;
      }
    } else if (newScale < 10) {
      newScale *= 1.1;
    }
    let box = $svg[0].getBoundingClientRect();
    x += (box.x - e.pageX + box.width / 2) * (newScale / prevScale - 1);
    y += (box.y - e.pageY + box.height / 2) * (newScale / prevScale - 1);
    transform(x, y, scale = newScale);
    return false;
  });
  return $svg;
}

function load() {
  viz = new Viz();
  loadPartitions();
}

window.onload = load;
