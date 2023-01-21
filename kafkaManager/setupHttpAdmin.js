function adminRequest (node, res, data, err) {
  if (err) {
    node.error(err)
    res.status(500).send(err)
    return
  }
  res.status(200).send(data)
}

function setupHttpAdmin (RED, nodeType, actions) {
  RED.httpAdmin.get('/' + nodeType.replace(' ', '_') + '/:id/:action/', RED.auth.needsPermission('KafkaAdmin.write'), function (req, res) {
    const node = RED.nodes.getNode(req.params.id)
    if (node && node.type === nodeType) {
      try {
        const action = req.params.action
        if (actions.hasOwnProperty(action)) {
          callFunction = actions[action].bind(node)
          callFunction(RED,node,(data,err)=>adminRequest(node, res, data, err))
          return
        }
        throw Error('unknown action: ' + action)
      } catch (ex) {
        adminRequest(node, res, 'Internal Server Error, ' + req.params.action + ' failed ' + ex.toString())
      }
    } else {
      res.status(404).send('request to ' + req.params.action + ' failed for id:' + req.params.id)
    }
  })
}
module.exports = setupHttpAdmin
