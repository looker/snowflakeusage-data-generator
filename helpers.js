export function randomFromArray (theArray) {
  return theArray[Math.floor(Math.random() * theArray.length)]
}

export function randomFromArrayByWeight (theArray) {
  let sumWeight = 0
  const r = Math.random()
  for (let item of theArray) {
    sumWeight += item.weight
    if (r <= sumWeight) return item.name
  }
}
